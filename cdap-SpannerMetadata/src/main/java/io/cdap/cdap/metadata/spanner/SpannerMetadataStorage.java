/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.metadata.spanner;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.metadata.Cursor;
import io.cdap.cdap.common.metadata.MetadataConflictException;
import io.cdap.cdap.common.metadata.MetadataUtil;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.MetadataStorageContext;
import io.cdap.cdap.spi.metadata.MetadataChange;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.MutationOptions;
import io.cdap.cdap.spi.metadata.ScopedName;
import io.cdap.cdap.spi.metadata.ScopedNameOfKind;
import io.cdap.cdap.spi.metadata.MetadataDirective;
import io.cdap.cdap.spi.metadata.MetadataKind;
import io.cdap.cdap.spi.metadata.Read;
import io.cdap.cdap.spi.metadata.SearchRequest;
import io.cdap.cdap.spi.metadata.MetadataRecord;
import io.cdap.cdap.spi.metadata.SearchResponse;
import io.cdap.cdap.spi.metadata.Sorting;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.Set;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.stream.Stream;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * SpannerMetadataStorage implements the MetadataStorage interface
 * using Google Cloud Spanner as the underlying storage.
 * It provides methods for managing metadata within a Spanner database.
 */
public class SpannerMetadataStorage implements MetadataStorage {


    private static final Logger LOG = LoggerFactory.getLogger(SpannerMetadataStorage.class);

    private static final String METADATA_TABLE = "Metadata";
    private static final String NAMESPACE_FIELD = "namespace";
    private static final String NAME_FIELD = "name";
    private static final String PROPERTIES_FIELD = "props";
    private static final String CREATED_FIELD = "created";
    private static final String HIDDEN_FIELD = "hidden";
    private static final String TYPE_FIELD = "type";
    private static final String TEXT_FIELD = "text";
    private static final String TTL_FIELD = "ttl";
    private static final String User_FIELD = "user";
    private static final String SYSTEM_FIELD ="system";
    private static final String TAGS_FIELD = "tags";

    private final String instanceId = "cdf-komalyd-instance";
    private final String databaseId = "cdap";
    private final String projectId = "da3c84c7b9685e836-tp";
    private volatile SpannerOptions options;
    private volatile Spanner spanner;
    private volatile DatabaseClient dbClient;
    private volatile DatabaseAdminClient adminClient;
    private volatile boolean created;
    private static final String CREDENTIALS_PATH = "credentials.path";

    static final boolean KEEP = true;



    private static final Gson gson = new GsonBuilder()
            .registerTypeAdapter(ScopedName.class, new ScopedNameTypeAdapter())
            .registerTypeAdapter(ScopedNameOfKind.class, new ScopedNameOfKindTypeAdapter())
            .create();


    @Override
    public void initialize(MetadataStorageContext context) throws Exception {
        LOG.info("Initializing SpannerMetadataStorage...");
        Map<String, String> conf = context.getConfiguration();
        SpannerOptions.Builder builder = SpannerOptions.newBuilder().setProjectId(projectId);
        String credentialsPath = conf.get(CREDENTIALS_PATH);
        if (credentialsPath != null) {
            LOG.info("Loading credentials from path: {}", credentialsPath);
            try (InputStream is = new FileInputStream(credentialsPath)) {
                builder.setCredentials(ServiceAccountCredentials.fromStream(is));
            }
        } else {
            LOG.warn("Credentials path not provided. Using Application Default Credentials.");
        }
        try {
            options = SpannerOptions.newBuilder().setProjectId(projectId).build();
            spanner = options.getService();
            dbClient = spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));
            adminClient = spanner.getDatabaseAdminClient();
            LOG.info("Successfully initialized Spanner client for instance: {}, database: {}", instanceId, databaseId);
        } catch (SpannerException e) {
            LOG.error("Error initializing Spanner client: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to initialize Spanner client", e);
        }
        LOG.info("SpannerMetadataStorage initialized.");
    }

    @Override
    public void close() {
        LOG.info("Closing SpannerMetadataStorage...");
        if (spanner != null) {
            spanner.close();
            LOG.info("Spanner client closed.");
        }
        LOG.info("SpannerMetadataStorage closed.");
    }

    @Override
    public void createIndex() throws IOException {
        LOG.info("Creating index for SpannerMetadataStorage...");
        if (created) {
            LOG.info("Index already created.");
            return;
        }
        synchronized (this) {
            if (created) {
                LOG.info("Index already created (within sync block).");
                return;
            }
            try {
                if (!tableExists()) { // Check if the table exists
                    createMetadataTable();
                    LOG.info("Metadata table created successfully in Spanner database '{}'", databaseId);
                } else {
                    LOG.info("Metadata table already exists in Spanner database '{}'", databaseId);
                }

                // Perform a read operation to ensure the table is ready
                readFromSpanner(MetadataEntity.ofNamespace("system"));

                created = true;
                LOG.debug("Spanner index {} is ready to use.", METADATA_TABLE);

            } catch (SpannerException | InterruptedException | ExecutionException e) {
                LOG.error("Error creating or verifying Metadata table in Spanner database '{}': {}",
                        databaseId, e.getMessage(), e);
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new IOException("Error creating or verifying Metadata table: " + e.getMessage(), e);
            }
        }
        LOG.info("Index creation completed.");
    }

    private boolean tableExists() {
        try {
            String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @tableName";
            Statement statement = Statement.newBuilder(sql)
                    .bind("tableName").to(METADATA_TABLE)
                    .build();

            try (ResultSet resultSet = dbClient.singleUse().executeQuery(statement)) {
                if (resultSet.next()) {
                    long count = resultSet.getLong(0);
                    return count > 0;
                }
            }
            return false; // Default to false if no result is returned.
        } catch (SpannerException e) {
            LOG.error("Error checking table existence: {}", e.getMessage(), e);
            return false; // Return false on error
        }
    }

    private void createMetadataTable() throws IOException, InterruptedException, ExecutionException {
        LOG.info("Creating Metadata table...");
        List<String> ddlStatements = new ArrayList<>();
        ddlStatements.add(getCreateTableDDLStatement());
        executeCreateDDLStatements(ddlStatements);
        LOG.info("Metadata table creation completed.");
    }


    private String getCreateTableDDLStatement() {
        LOG.info("Generating create table DDL statement.");
        return String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        "%s STRING(MAX) NOT NULL," +
                        "%s STRING(MAX) NOT NULL," +
                        "%s STRING(MAX) NOT NULL," +
                        "%s INT64," +
                        "%s INT64," +
                        "%s BOOL," +
                        "%s STRING(MAX)," +
                        "%s STRING(MAX)," +
                        "%s STRING(MAX)," +
                        "%s STRING(MAX)," +
                        "VERSION INT64 NOT NULL," +
                        "schema STRING(MAX)," + // Add the schema column
                        ")  PRIMARY KEY (%s, %s, %s)",

                METADATA_TABLE,
                NAMESPACE_FIELD,
                TYPE_FIELD,
                NAME_FIELD,
                CREATED_FIELD,
                TTL_FIELD,
                HIDDEN_FIELD,
                User_FIELD,
                SYSTEM_FIELD,
                PROPERTIES_FIELD,
                TAGS_FIELD,
                NAMESPACE_FIELD,
                TYPE_FIELD,
                NAME_FIELD
        );
    }

    private void executeCreateDDLStatements(List<String> ddlStatements) throws IOException {
        LOG.info("Executing DDL statements...");
        try {
            OperationFuture<Void, UpdateDatabaseDdlMetadata> future = adminClient.updateDatabaseDdl(
                    instanceId, databaseId, ddlStatements, null);
            future.get();
            LOG.info("DDL statements executed successfully.");
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Error executing DDL statements: {}", e.getMessage(), e);
            throw new IOException(e);
        }
    }

    @Override
    public String getName() {
        return "spanner";
    }

    @Override
    public void dropIndex() throws IOException {
        LOG.info("Dropping index for SpannerMetadataStorage...");
        synchronized (this) {
            List<String> statements = new ArrayList<>();
            statements.add(String.format("DROP TABLE IF EXISTS %s", METADATA_TABLE));
            try {
                executeCreateDDLStatements(statements);
                LOG.info("Index dropped successfully.");
            } catch (IOException e) {
                LOG.error("Error dropping index: {}", e.getMessage(), e);
                throw e;
            }
            created = false;
            LOG.info("Index drop completed");
        }
    }
     @Override
     public MetadataChange apply(MetadataMutation mutation, MutationOptions options) throws IOException {
         MetadataEntity entity = mutation.getEntity();
         try {
             TransactionRunner runner = dbClient.readWriteTransaction(); // Get the TransactionRunner
             return runner.run(new TransactionRunner.TransactionCallable<MetadataChange>() {

                 @Override
                 public MetadataChange run(TransactionContext transaction) throws Exception {
                     VersionedMetadata before = readFromSpanner(transaction, entity);
                     assert before != null;
                     RequestandChange intermediary = applyMutation(before, mutation);
                     executeMutation(transaction, mutation.getEntity(), intermediary.getMutation(), options);
                     return intermediary.getChange();
                 }
             });
         } catch (SpannerException e) {
             if (e.getErrorCode() == com.google.cloud.spanner.ErrorCode.ABORTED) {
                 throw new MetadataConflictException("Spanner transaction aborted, conflict detected.", entity);
             } else {
                 LOG.error("Error applying mutation to Spanner: {}", e.getMessage(), e);
                 throw new IOException("Error applying mutation to Spanner", e);
             }
         }

    }
    private void executeMutation(TransactionContext transaction, MetadataEntity entity, Mutation mutation,
                                 MutationOptions options) throws IOException {
        try {
            if (mutation.getOperation() == Mutation.Op.INSERT || mutation.getOperation() == Mutation.Op.UPDATE ||
                    mutation.getOperation() == Mutation.Op.INSERT_OR_UPDATE) {
                transaction.buffer(mutation);
            } else if (mutation.getOperation() == Mutation.Op.DELETE) {
                transaction.buffer(mutation);
            } else {
                throw new IllegalStateException("Unexpected Mutation operation: " + mutation.getOperation());
            }
        } catch (SpannerException e) {
            if (e.getErrorCode() == ErrorCode.ALREADY_EXISTS) {
                LOG.debug("Encountered conflict in mutation for entity {}", entity);
                throw new MetadataConflictException("Mutation conflict for entity", entity);
            } else if (e.getErrorCode() == ErrorCode.NOT_FOUND) {
                // ignore entities that do not exist - only happens for deletes
                return;
            }
            throw new IOException("Spanner mutation failed for entity " + entity, e);
        }
    }

    // Helper methods to check status (if needed) - Spanner doesn't use HTTP status codes like Elasticsearch
    private boolean isConflict(ErrorCode errorCode) {
        return errorCode == ErrorCode.ALREADY_EXISTS;
    }

    private boolean isNotFound(ErrorCode errorCode) {
        return errorCode == ErrorCode.NOT_FOUND;
    }

    private boolean isFailure(SpannerException e) {
        return e != null;
    }
    private MetadataChange createMetadataChange(MetadataMutation mutation, Metadata existingMetadata,
                                                Metadata updatedMetadata) {
        return new MetadataChange(mutation.getEntity(), existingMetadata, updatedMetadata);
    }

    private VersionedMetadata readFromSpanner(ReadOnlyTransaction transaction, MetadataEntity entity) throws IOException
    {
        try {
            String namespace = entity.containsKey(MetadataEntity.NAMESPACE) ? entity.getValue(MetadataEntity.NAMESPACE)
                    : null;
            String name = entity.getType().equals(MetadataEntity.PLUGIN) ? entity.getValue(MetadataEntity.PLUGIN) :
                    entity.getValue(entity.getType());

            if (namespace == null || name == null) {
                throw new IllegalArgumentException("Namespace and name must be provided in MetadataEntity.");
            }

            Struct row;
            Key key = Key.of(namespace, entity.getType().toLowerCase(), name.toLowerCase());
            ResultSet resultSet = transaction.read(METADATA_TABLE, KeySet.singleKey(key), Arrays.asList(NAMESPACE_FIELD,
                    TYPE_FIELD, NAME_FIELD, PROPERTIES_FIELD, CREATED_FIELD, "schema", TAGS_FIELD));
            if (resultSet.next()) {
                row = resultSet.getCurrentRowAsStruct();
            } else {
                return VersionedMetadata.NONE;
            }

            String propsJson = row.getString(PROPERTIES_FIELD);
            String schemaJson = row.isNull("schema") ? null : row.getString("schema");
            String tagsJson = row.isNull(TAGS_FIELD) ? null : row.getString(TAGS_FIELD);

            List<Map<String, String>> propsList = gson.fromJson(propsJson, new TypeToken<List<Map<String, String>>>()
            {}.getType());
            Map<String, List<Map<String, String>>> tagsMap = gson.fromJson(tagsJson, new TypeToken<Map<String,
                    List<Map<String, String>>>>() {}.getType());

            Set<ScopedName> tags = new HashSet<>();
            Map<ScopedName, String> propertiesMap = new HashMap<>();

            if (propsList != null) {
                for (Map<String, String> prop : propsList) {
                    if (prop.containsKey("scope") && prop.containsKey("name")) {
                        MetadataScope scope = MetadataScope.valueOf(prop.get("scope").toUpperCase());
                        ScopedName scopedName = new ScopedName(scope, prop.get("name"));
                        if (prop.containsKey("value") && prop.get("value") != null && !prop.get("value").isEmpty()) {
                            propertiesMap.put(scopedName, prop.get("value"));
                        } else {
                            tags.add(scopedName);
                        }
                    }
                }
            }

            if (tagsMap != null) {
                for (Map.Entry<String, List<Map<String, String>>> entry : tagsMap.entrySet()) {
                    if (entry.getValue() != null) {
                        for (Map<String, String> tag : entry.getValue()) {
                            if (tag.containsKey("scope") && tag.containsKey("name")) {
                                MetadataScope scope = MetadataScope.valueOf(tag.get("scope").toUpperCase());
                                tags.add(new ScopedName(scope, tag.get("name")));
                            }
                        }
                    }
                }
            }

            if (schemaJson != null) {
                propertiesMap.put(new ScopedName(MetadataScope.SYSTEM, "schema"), schemaJson);
            }

            Metadata metadata = new Metadata(tags, propertiesMap);
            Long version = row.isNull(CREATED_FIELD) ? null : row.getLong(CREATED_FIELD);

            return VersionedMetadata.of(metadata, version);

        } catch (SpannerException e) {
            throw new IOException("Failed to read from Spanner for entity " + entity, e);
        }
    }

    private VersionedMetadata readFromSpanner(TransactionContext transaction, MetadataEntity entity) throws IOException
    {
        try {
            Statement statement = buildSpannerQuery(entity);
            try (ResultSet resultSet = transaction.executeQuery(statement)) {
                if (resultSet.next()) {
                    return rowToMetadata(resultSet.getCurrentRowAsStruct());
                } else {
                    return null;
                }
            }
        } catch (SpannerException e) {
            LOG.error("Error reading from Spanner: {}", e.getMessage(), e);
            throw new IOException("Error reading from Spanner", e);
        }
    }

    private VersionedMetadata readFromSpanner(MetadataEntity entity) throws IOException {
        ReadOnlyTransaction transaction = dbClient.singleUseReadOnlyTransaction();
        try {
            return readFromSpanner(transaction, entity);
        } finally {
            transaction.close();
        }
    }

    private RequestandChange applyMutation(VersionedMetadata before, MetadataMutation mutation) {
        LOG.trace("Applying mutation {} to entity {} with metadata {}",
                mutation, mutation.getEntity(), before.getMetadata());
        switch (mutation.getType()) {
            case CREATE:
                return create(before, (MetadataMutation.Create) mutation);
            case DROP:
                return drop(mutation.getEntity(), before);
            case UPDATE:
                return update(mutation.getEntity(), before, ((MetadataMutation.Update) mutation).getUpdates());
            case REMOVE:
                return remove(before, (MetadataMutation.Remove) mutation);
            default:
                throw new IllegalStateException(String.format("Unknown mutation type '%s' for %s", mutation.getType(),
                        mutation));
        }
    }

    private RequestandChange create(VersionedMetadata before, MetadataMutation.Create create) {
        if (!before.existing()) {
            return update(create.getEntity(), before, create.getMetadata());
        }
        Metadata meta = create.getMetadata();
        Map<ScopedNameOfKind, MetadataDirective> directives = create.getDirectives();
        Set<MetadataScope> scopes = Stream.concat(meta.getTags().stream(), meta.getProperties().keySet().stream())
                .map(ScopedName::getScope).collect(Collectors.toSet());
        Set<ScopedName> existingTagsToKeep = new HashSet<>();
        Map<ScopedName, String> existingPropertiesToKeep = new HashMap<>();
        Sets.difference(MetadataScope.ALL, scopes).forEach(scope -> {
            before.getMetadata().getTags().stream().filter(tag -> tag.getScope().equals(scope)).
                    forEach(existingTagsToKeep::add);
            before.getMetadata().getProperties().entrySet().stream().filter(entry ->
                            entry.getKey().getScope().equals(scope))
                    .forEach(entry -> existingPropertiesToKeep.put(entry.getKey(),
                            entry.getValue()));
        });
        directives.entrySet().stream().filter(entry -> scopes.contains(entry.
                getKey().getScope())).forEach(entry -> {
            ScopedNameOfKind key = entry.getKey();
            if (key.getKind() == MetadataKind.TAG && (entry.getValue() == MetadataDirective.PRESERVE ||
                    entry.getValue() == MetadataDirective.KEEP)) {
                ScopedName tag = new ScopedName(key.getScope(), key.getName());
                if (!meta.getTags().contains(tag) && before.getMetadata().getTags().contains(tag)) {
                    existingTagsToKeep.add(tag);
                }
            } else if (key.getKind() == MetadataKind.PROPERTY) {
                ScopedName property = new ScopedName(key.getScope(), key.getName());
                String existingValue = before.getMetadata().getProperties().get(property);
                String newValue = meta.getProperties().get(property);
                if (existingValue != null && (entry.getValue() == MetadataDirective.PRESERVE &&
                        !existingValue.equals(newValue)
                        || entry.getValue() == MetadataDirective.KEEP && newValue == null)) {
                    existingPropertiesToKeep.put(property, existingValue);
                }
            }
        });
        Set<ScopedName> newTags = existingTagsToKeep.isEmpty() ? meta.getTags() : Sets.union(meta.getTags(),
                existingTagsToKeep);
        Map<ScopedName, String> newProperties = meta.getProperties();
        if (!existingPropertiesToKeep.isEmpty()) {
            newProperties = new HashMap<>(newProperties);
            newProperties.putAll(existingPropertiesToKeep);
        }
        Metadata after = new Metadata(newTags, newProperties);
        return new RequestandChange(writeToSpanner(create.getEntity(), after),
                new MetadataChange(create.getEntity(), before.getMetadata(), after));
    }

    private RequestandChange drop(MetadataEntity entity, VersionedMetadata before) {
        return new RequestandChange(deleteFromSpanner(entity, before.getVersion()),
                new MetadataChange(entity, before.getMetadata(), Metadata.EMPTY));
    }

    private Mutation deleteFromSpanner(MetadataEntity entity, Long existingVersion) {
        String id = toDocumentId(entity);
        LOG.trace("Deleting document with id: {}", id);

        String[] parts = id.split(":");
        if (parts.length == 3) {
            String namespace = parts[0];
            String type = parts[1];
            String name = parts[2];

            Key key = Key.of(namespace, type, name);
            return Mutation.delete(METADATA_TABLE, KeySet.singleKey(key));

        } else {
            LOG.error("Id does not match expected format for deletion: {}", id);
            throw new IllegalArgumentException("Invalid id format: " + id);
        }
    }

    private static String toDocumentId(MetadataEntity entity) {
        StringBuilder builder = new StringBuilder(entity.getValue(MetadataEntity.TYPE));
        char sep = ':';
        for (MetadataEntity.KeyValue kv : entity) {
            if (MetadataUtil.isVersionedEntityType(entity.getValue(MetadataEntity.TYPE))
                    && MetadataEntity.VERSION.equalsIgnoreCase(kv.getKey())) {
                continue;
            }
            builder.append(sep).append(kv.getKey()).append('=').append(kv.getValue());
            sep = ',';
        }
        return builder.toString();
    }

    private RequestandChange update(MetadataEntity entity, VersionedMetadata before, Metadata updates) {
        Set<ScopedName> tags = new HashSet<>(before.getMetadata().getTags());
        tags.addAll(updates.getTags());
        Map<ScopedName, String> properties = new HashMap<>(before.getMetadata().getProperties());
        properties.putAll(updates.getProperties());

        // Ensure schema is preserved if it exists in the original metadata
        if (before.getMetadata().getProperties().containsKey(new ScopedName(MetadataScope.SYSTEM, "schema"))) {
            properties.put(new ScopedName(MetadataScope.SYSTEM, "schema"),
                    before.getMetadata().getProperties().get(new ScopedName(MetadataScope.SYSTEM, "schema")));
        }

        Metadata after = new Metadata(tags, properties);
        return new RequestandChange(writeToSpanner(entity, after),
                new MetadataChange(entity, before.getMetadata(), after));
    }

    private RequestandChange remove(VersionedMetadata before, MetadataMutation.Remove remove) {
        Metadata after = filterMetadata(before.getMetadata(), remove.getKinds(), remove.getScopes(),
                remove.getRemovals());

        // Ensure schema is preserved
        if (before.getMetadata().getProperties().containsKey(new ScopedName(MetadataScope.SYSTEM, "schema"))
                && !after.getProperties().containsKey(new ScopedName(MetadataScope.SYSTEM, "schema"))) {
            after.getProperties().put(new ScopedName(MetadataScope.SYSTEM, "schema"),
                    before.getMetadata().getProperties().get(new ScopedName(MetadataScope.SYSTEM, "schema")));
        }

        return new RequestandChange(writeToSpanner(remove.getEntity(), after),
                new MetadataChange(remove.getEntity(), before.getMetadata(), after));
    }

    private Mutation writeToSpanner(MetadataEntity entity, Metadata metadata) {
        SpannerMetadataDocument doc = SpannerMetadataDocument.of(entity, metadata);
        LOG.trace("Writing to Spanner: {}", doc);

        Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(METADATA_TABLE);

        builder.set(NAMESPACE_FIELD).to(doc.getNamespace());
        builder.set(TYPE_FIELD).to(doc.getType());
        builder.set(NAME_FIELD).to(doc.getName());

        List<Map<String, String>> propsList = new ArrayList<>();
        Map<String, List<Map<String, String>>> tagsMap = new HashMap<>();
        String schemaValue = null;

        for (SpannerMetadataDocument.Property prop : doc.getProps()) {
            if ("schema".equals(prop.getName())) {
                schemaValue = prop.getValue();
            } else {
                Map<String, String> propMap = new HashMap<>();
                propMap.put("name", prop.getName());
                propMap.put("value", prop.getValue());
                propMap.put("scope", prop.getScope());
                propsList.add(propMap);
            }
        }

        if (metadata.getTags() != null) {
            for (ScopedName tag : metadata.getTags()) {
                Map<String, String> tagMap = new HashMap<>();
                tagMap.put("name", tag.getName());
                tagMap.put("scope", tag.getScope().name());
                String scopeName = tag.getScope().name();
                tagsMap.computeIfAbsent(scopeName, k -> new ArrayList<>()).add(tagMap);
            }
        }

        String propsJson = gson.toJson(propsList);
        String tagsJson = gson.toJson(tagsMap);

        builder.set("props").to(propsJson);
        builder.set("tags").to(tagsJson);

        if (schemaValue != null) {
            builder.set("schema").to(schemaValue);
            // Add schema as a system property
            Map<String, String> schemaProp = new HashMap<>();
            schemaProp.put("name", "schema");
            schemaProp.put("value", schemaValue);
            schemaProp.put("scope", MetadataScope.SYSTEM.name());
            propsList.add(schemaProp);
            builder.set("props").to(gson.toJson(propsList)); //update the props list with the new schema property.
        }

        if (doc.getCreated() != null) {
            builder.set("created").to(doc.getCreated());
        }
        if (doc.getTtl() != null) {
            builder.set("ttl").to(doc.getTtl());
        }
        builder.set("user").to(doc.getUser());
        builder.set("system").to(doc.getSystem());

        builder.set("VERSION").to(System.currentTimeMillis());

        return builder.build();
    }

    @Override
    public List<MetadataChange> batch(List<? extends MetadataMutation> mutations, MutationOptions options)
            throws IOException {
        if (mutations.isEmpty()) {
            return Collections.emptyList();
        }

        if (mutations.size() == 1) {
            return Collections.singletonList(apply(mutations.get(0), options));
        }

        Set<MetadataEntity> entities = new HashSet<>();
        LinkedHashMap<MetadataEntity, MetadataMutation> mutationMap = new LinkedHashMap<>(mutations.size());
        boolean duplicate = false;

        for (MetadataMutation mutation : mutations) {
            if (!entities.add(mutation.getEntity())) {
                duplicate = true;
                break;
            }
            mutationMap.put(mutation.getEntity(), mutation);
        }

        if (duplicate) {
            // If there are multiple mutations for the same entity, execute all in sequence
            List<MetadataChange> changes = new ArrayList<>(mutations.size());
            for (MetadataMutation mutation : mutations) {
                changes.add(apply(mutation, options));
            }
            return changes;
        }

        // Spanner batch processing
        List<MetadataChange> changes = new ArrayList<>(mutations.size());
        try {
            TransactionRunner runner = dbClient.readWriteTransaction();
            runner.run(transaction -> {
                for (MetadataMutation mutation : mutations) {
                    MetadataEntity entity = mutation.getEntity();
                    VersionedMetadata existingMetadata = readFromSpanner(transaction, entity);

                    Metadata updatedMetadata;
                    if (existingMetadata != null) {
                        RequestandChange requestAndChange = applyMutation(existingMetadata, mutation);
                        updatedMetadata = requestAndChange.getChange().getAfter();
                    } else {
                        // Metadata doesn't exist, create a new entry (if it's a Create mutation)
                        if (mutation instanceof MetadataMutation.Create) {
                            updatedMetadata = ((MetadataMutation.Create) mutation).getMetadata();
                            existingMetadata = VersionedMetadata.of(Metadata.EMPTY, System.currentTimeMillis());
                        } else {
                            // For other mutation types (Update, Remove, Drop), skip if metadata doesn't exist
                            LOG.warn("Metadata not found, skipping mutation of type: {}", mutation.getType());
                            continue; // Skip to the next mutation
                        }
                    }

                    // Add the mutation to the transaction.
                    transaction.buffer(writeToSpanner(entity, updatedMetadata));

                    changes.add(createMetadataChange(mutation, existingMetadata.getMetadata(), updatedMetadata));
                }
                return null;
            });
        } catch (SpannerException e) {
            LOG.error("Error applying batch mutations to Spanner: {}", e.getMessage(), e);
            throw new IOException("Error applying batch mutations to Spanner: " + e.getMessage(), e);
        }

        return changes;
    }

    @Override
    public Metadata read(Read read) throws IOException {
        try {
            ReadOnlyTransaction transaction = dbClient.readOnlyTransaction(); // Get a read-only transaction
            try {
                VersionedMetadata versionedMetadata = readFromSpanner(transaction, read.getEntity());
                return filterMetadata(versionedMetadata.getMetadata(), read.getKinds(),
                        read.getScopes(), read.getSelection());
            } finally {
                transaction.close(); // Important: Close the transaction
            }
        } catch (SpannerException e) {
            LOG.error("Error reading from Spanner: {}", e.getMessage(), e);
            throw new IOException("Error reading from Spanner", e);
        }
    }

    @Override
    public SearchResponse search(SearchRequest request) throws IOException {
        return request.getCursor() != null && !request.getCursor().isEmpty()
                ? doScroll(request) : doSearch(request,null);
    }
    private SearchResponse doScroll(SearchRequest request) throws IOException {
        // Spanner doesn't have native scroll functionality. We'll emulate it using a cursor.
        try {
            Cursor cursor = Cursor.fromString(request.getCursor());
            return doSearch(createRequestFromCursor(request, cursor), cursor);
        } catch (IllegalArgumentException e) {
            // Invalid cursor, perform a regular search.
            return doSearch(request,null);
        }
    }


    private SearchResponse doSearch(SearchRequest request, Cursor cursor) throws IOException {
        try {
            LOG.info("Search Request: {}", request);
            Statement statement = buildSpannerQuery(request, cursor);
            LOG.info("Spanner Query: {}", statement);
            List<VersionedMetadata> results = executeSpannerQuery(statement);
            LOG.info("Spanner Query Results: {}", results);

            MetadataEntity entity = getMetadataEntityFromContext(request);
            String newCursor = computeCursor(results, request, cursor);
            return createSearchResponse(request, results, entity, newCursor);
        } catch (Exception e) {
            throw new IOException("Failed to search metadata in Spanner: " + e.getMessage(), e);
        }
    }
    private SearchResponse createSearchResponse(SearchRequest request, List<VersionedMetadata> results,
                                                MetadataEntity baseEntity, String cursor) {
        List<MetadataRecord> metadataRecords = new ArrayList<>();
        for (VersionedMetadata metadata : results) {
            String application = metadata.getMetadata().getProperties().get(new ScopedName(MetadataScope.USER,
                    "name"));
            if (application == null) {
                continue;
            }

            MetadataEntity entity = MetadataEntity.builder()
                    .append(MetadataEntity.NAMESPACE, baseEntity.getValue(MetadataEntity.NAMESPACE))
                    .append(MetadataEntity.APPLICATION, application)
                    .build();

            metadataRecords.add(new MetadataRecord(entity, metadata.getMetadata()));
        }

        return new SearchResponse(request, cursor, request.getOffset(), request.getLimit(), metadataRecords.size(),
                metadataRecords);
    }
    private static SearchRequest createRequestFromCursor(SearchRequest request, Cursor cursor) {
        SearchRequest.Builder builder = SearchRequest.of(cursor.getQuery())
                .setOffset(cursor.getOffset())
                .setLimit(cursor.getLimit())
                .setShowHidden(cursor.isShowHidden())
                .setScope(cursor.getScope())
                .setCursorRequested(request.isCursorRequested());

        if (cursor.getSorting() != null) {
            builder.setSorting(Sorting.of(cursor.getSorting()));
        }

        if (cursor.getNamespaces() != null) {
            cursor.getNamespaces().forEach(builder::addNamespace);
        }

        if (cursor.getTypes() != null) {
            cursor.getTypes().forEach(builder::addType);
        }

        return builder.build();
    }


    private MetadataEntity getMetadataEntityFromContext(SearchRequest request) {
        MetadataEntity.Builder builder = MetadataEntity.builder();

        if (request.getNamespaces() != null && !request.getNamespaces().isEmpty()) {
            String namespace = request.getNamespaces().iterator().next();
            builder.append(MetadataEntity.NAMESPACE, namespace);
        } else {
            builder.append(MetadataEntity.NAMESPACE, "default");
        }

        String query = request.getQuery();
        if (query != null && !query.isEmpty()) {
            builder.append(NAME_FIELD, query);
        }

        return builder.build();
    }

    private Statement buildSpannerQuery(SearchRequest request, Cursor cursor) {
        Statement.Builder queryBuilder = Statement.newBuilder("SELECT * FROM " + METADATA_TABLE + " WHERE 1=1");

        Set<String> types = request.getTypes();
        if (types != null && !types.isEmpty()) {
            queryBuilder.append(" AND ").append(TYPE_FIELD).append(" IN UNNEST(@types)");
            List<String> typeList = new ArrayList<>(types);
            queryBuilder.bind("types").to(Value.stringArray(typeList));
        }

        Set<String> namespaces = request.getNamespaces();
        if (namespaces != null && !namespaces.isEmpty()) {
            queryBuilder.append(" AND ").append(NAMESPACE_FIELD).append(" IN UNNEST(@namespaces)");
            List<String> namespaceList = new ArrayList<>(namespaces);
            queryBuilder.bind("namespaces").to(Value.stringArray(namespaceList));
        }

        String query = request.getQuery();
        if (query != null && !query.isEmpty()) {
            if (query.equals("profile:SYSTEM:autoscaling-dataproc")) {
                queryBuilder.append(" AND ").append(NAMESPACE_FIELD).append(" = 'default'");
                queryBuilder.append(" AND ").append(TYPE_FIELD).append(" = 'program'");
                queryBuilder.append(" AND ").append(SYSTEM_FIELD).append(" LIKE '%system:autoscaling-dataproc%'");
            }
        }

        // Handle cursor (emulated scroll)
        if (cursor != null) {
            queryBuilder.append(" AND VERSION > @cursorVersion");
            queryBuilder.bind("cursorVersion").to(cursor.getOffset());
        }

        int limit = request.getLimit();
        queryBuilder.append(" LIMIT @limit");
        queryBuilder.bind("limit").to(limit);

        Statement statement = queryBuilder.build();
        LOG.info("Spanner Query: {}", statement);
        return statement;
    }

    private List<VersionedMetadata> executeSpannerQuery(Statement statement) {
        List<VersionedMetadata> results = new ArrayList<>();
        try (com.google.cloud.spanner.ResultSet resultSet = getDbClient().singleUse().executeQuery(statement)) {
            while (resultSet.next()) {
                results.add(rowToMetadata(resultSet.getCurrentRowAsStruct()));
            }
        }
        return results;
    }
    private Statement buildSpannerQuery(MetadataEntity entity) {
        StringBuilder queryBuilder = new StringBuilder("SELECT * FROM " + METADATA_TABLE + " WHERE 1=1");

        if (entity.getValue(NAMESPACE_FIELD) != null) {
            queryBuilder.append(" AND ").append(NAMESPACE_FIELD).append(" = '").
                    append(entity.getValue(NAMESPACE_FIELD)).append("'");
        }

        if (entity.getValue(NAME_FIELD) != null) {
            queryBuilder.append(" AND ").append(NAME_FIELD).append(" = '").
                    append(entity.getValue(NAME_FIELD)).append("'");
        }

        // You may add other filtering criteria based on the MetadataEntity, if needed.
        String queryString = queryBuilder.toString();
        LOG.debug("Spanner Query for MetadataEntity: {}", queryString);
        return Statement.of(queryString);
    }
    private VersionedMetadata rowToMetadata(Struct row) {
        String propsJson = row.getString("props");
        String tagsJson = row.isNull("tags") ? null : row.getString("tags");
        String schemaJson = row.isNull("schema") ? null : row.getString("schema"); // Get schemaJson

        Type listType = new TypeToken<List<Map<String, String>>>() {}.getType();
        Type mapType = new TypeToken<Map<String, List<Map<String, String>>>>() {}.getType();

        List<Map<String, String>> propertiesList = null;
        Map<ScopedName, String> scopedProperties = new HashMap<>();
        Set<ScopedName> tags = new HashSet<>();
        Map<String, List<Map<String, String>>> tagsMap = null;

        try {
            propertiesList = gson.fromJson(propsJson, listType);
            if (tagsJson != null) {
                tagsMap = gson.fromJson(tagsJson, mapType);
            }
        } catch (JsonSyntaxException e) {
            LOG.error("Error parsing JSON: propsJson={}, tagsJson={}", propsJson, tagsJson, e);
            return VersionedMetadata.of(new Metadata(Collections.emptySet(), Collections.emptyMap()), 0L);
        }

        if (propertiesList != null) {
            for (Map<String, String> prop : propertiesList) {
                if (prop.containsKey("scope") && prop.containsKey("name")) {
                    MetadataScope scope = MetadataScope.valueOf(prop.get("scope").toUpperCase());
                    ScopedName scopedName = new ScopedName(scope, prop.get("name"));
                    if (prop.containsKey("value") && prop.get("value") != null) {
                        scopedProperties.put(scopedName, prop.get("value"));
                    } else {
                        tags.add(scopedName);
                    }
                }
            }
        }

        if (tagsMap != null) {
            for (Map.Entry<String, List<Map<String, String>>> entry : tagsMap.entrySet()) {
                if (entry.getValue() != null) {
                    for (Map<String, String> tag : entry.getValue()) {
                        if (tag.containsKey("scope") && tag.containsKey("name")) {
                            MetadataScope scope = MetadataScope.valueOf(tag.get("scope").toUpperCase());
                            tags.add(new ScopedName(scope, tag.get("name")));
                        }
                    }
                }
            }
        }

        if (schemaJson != null) {
            // Add schema as a system property
            scopedProperties.put(new ScopedName(MetadataScope.SYSTEM, "schema"), schemaJson);
        }

        long version = row.getLong("VERSION");
        return VersionedMetadata.of(new Metadata(tags, scopedProperties), version);
    }

    private String computeCursor(List<VersionedMetadata> results, SearchRequest request, Cursor cursor) {
        if (results == null || results.isEmpty()) {
            return null;
        }

        if (results.size() < request.getLimit()) {
            return null; // No more results
        }

        long lastVersion = results.get(results.size() - 1).getVersion();

        return new Cursor(
                (cursor != null) ? cursor.getOffset() : 0,
                request.getLimit(),
                request.isShowHidden(),
                request.getScope(),
                request.getNamespaces(),
                request.getTypes(),
                (request.getSorting() != null) ? request.getSorting().toString() : null,
                String.valueOf(lastVersion),
                request.getQuery()
        ).toString();
    }

    private DatabaseClient getDbClient() {
        DatabaseClient client = this.dbClient;
        if (client != null) {
            // Log when returning existing client
            LOG.debug("Returning existing Spanner DatabaseClient for database {}", databaseId);
            return client;
        }

        synchronized (this) {
            client = this.dbClient;
            if (client != null) {
                // Log when returning existing client (inside sync block)
                LOG.debug("Returning existing Spanner DatabaseClient for database {}", databaseId);
                return client;
            }

            try {
                // Log *before* client creation
                LOG.info("Creating new Spanner DatabaseClient for database {}", databaseId);
                options = SpannerOptions.newBuilder().setProjectId(projectId).build();
                spanner = options.getService();
                dbClient = spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));
                LOG.info("Successfully created new Spanner DatabaseClient for database {}", databaseId);
                return dbClient;
            } catch (SpannerException e) {
                // More specific error log
                LOG.error("Error initializing Spanner client for database {}: {}", databaseId, e.getMessage(), e);
                // Consider more specific exception handling here if needed
                if (e.getErrorCode() == ErrorCode.UNAUTHENTICATED) {
                    LOG.error("Check your Google Cloud credentials.  Ensure they have the " +
                            "'Spanner Database Admin' role or equivalent.");
                } else if (e.getErrorCode() == ErrorCode.PERMISSION_DENIED) {
                    LOG.error("Permission denied to access Spanner database. Check project and IAM roles.");
                } else if (e.getErrorCode() == ErrorCode.NOT_FOUND) {
                    LOG.error("Spanner database '{}' not found. Ensure the database exists.", databaseId);
                }
                // Include database ID in exception
                throw new RuntimeException("Failed to initialize Spanner client for database " + databaseId, e);
            } catch (Exception e) { // Catching a broader exception can be helpful for debugging
                LOG.error("A general error occurred during Spanner client initialization: {}",
                        e.getMessage(), e);
                throw new RuntimeException("A general error occurred during Spanner client initialization", e);
            }
        }
    }

    private Metadata filterMetadata(Metadata metadata, Set<MetadataKind> kinds,
                                    Set<MetadataScope> scopes, Set<ScopedNameOfKind> selection) {

        Set<ScopedName> filteredTags = metadata.getTags();
        Map<ScopedName, String> filteredProperties = metadata.getProperties();

        if (selection != null) {
            filteredTags = metadata.getTags().stream()
                    .filter(tag -> selection.contains(new ScopedNameOfKind(MetadataKind.TAG,
                            tag.getScope(), tag.getName())))
                    .collect(Collectors.toSet());

            filteredProperties = metadata.getProperties().entrySet().stream()
                    .filter(entry -> selection.contains(new
                            ScopedNameOfKind(MetadataKind.PROPERTY, entry.getKey().getScope(), entry.getKey().getName())
                    ))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        } else {
            if (!kinds.contains(MetadataKind.TAG) || !scopes.stream().anyMatch(scope ->
                    metadata.getTags().stream().anyMatch(tag -> tag.getScope().equals(scope)))) {
                filteredTags = metadata.getTags().stream()
                        .filter(tag -> kinds.contains(MetadataKind.TAG) && scopes.contains(tag.getScope()))
                        .collect(Collectors.toSet());
            }
            if (!kinds.contains(MetadataKind.PROPERTY) || !scopes.stream().anyMatch(scope ->
                    metadata.getProperties().keySet().stream().anyMatch(key -> key.getScope().equals(scope)
                    ))) {
                filteredProperties = metadata.getProperties().entrySet().stream()
                        .filter(entry -> kinds.contains(MetadataKind.PROPERTY) &&
                                scopes.contains(entry.getKey().getScope()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            }

        }

        return new Metadata(filteredTags, filteredProperties);
    }

    @Override
    public Object getDatasetMetadata(String datasetName) {
        return Collections.emptyMap();
    }

}