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
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.ReadContext;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.metadata.MetadataConflictException;
import io.cdap.cdap.common.metadata.MetadataUtil;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
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



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.LinkedHashMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;


import java.util.concurrent.TimeUnit;


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
    private static final String Entity_FIELD = "entity";
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
    private final String pipelineId="pipelineId";
    private static final int DEFAULT_PAGE_SIZE = 10;

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

            try (ResultSet resultSet = getDbClient().singleUse().executeQuery(statement)) {
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
                        "metadata STRING(MAX)," +
                        "%s STRING(MAX) NOT NULL," +
                        "%s STRING(MAX) NOT NULL," +
                        "%s STRING(MAX) NOT NULL," +
                        "%s INT64," +
                        "%s INT64," +
                        "%s BOOL," +
                        "%s STRING(MAX)," +
                        "%s STRING(MAX)," +
                        "props STRING(MAX)," +
                        "VERSION INT64," +
                        "%s STRING(MAX) ," +
                        ")  PRIMARY KEY (%s,%s,%s)",

                METADATA_TABLE,
                Entity_FIELD,
                NAMESPACE_FIELD,
                TYPE_FIELD,
                NAME_FIELD,
                CREATED_FIELD,
                TTL_FIELD,
                HIDDEN_FIELD,
                User_FIELD,
                SYSTEM_FIELD,
                pipelineId,
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

    public void dropIndex() throws IOException {
        try {
            getDbClient().readWriteTransaction().run(transaction -> {
                String sql = "DELETE FROM Metadata WHERE " + pipelineId + " = @pipelineId";
                Statement statement = Statement.newBuilder(sql).bind("pipelineId").to(pipelineId).build();
                long rowCount = transaction.executeUpdate(statement);
                LOG.info(rowCount + " rows deleted for pipeline " + pipelineId);
                return null;
            });
        } catch (SpannerException e) {
            throw new IOException("Failed to delete pipeline data: " + e.getMessage());
        }
    }

    @Override
    public MetadataChange apply(MetadataMutation mutation, MutationOptions options) throws IOException {
        MetadataEntity entity = mutation.getEntity();
        try {
            return getDbClient().readWriteTransaction().run(transaction -> {
                VersionedMetadata before = readFromSpanner(transaction, entity);
                RequestandChange intermediary = applyMutation(before, mutation);
                executeMutation(entity, intermediary.getMutation());
                return intermediary.getChange();
            });
        } catch (SpannerException e) {
            if (e.getErrorCode() == ErrorCode.ABORTED) {
                // Construct a MetadataConflictException with a List<MetadataEntity>
                List<MetadataEntity> conflictingEntities = new ArrayList<>();

                conflictingEntities.add(mutation.getEntity());
                throw new MetadataConflictException("Spanner transaction aborted: " + e.getMessage(),
                        conflictingEntities);
            } else {
                throw new IOException("Spanner error: " + e.getMessage());
            }
        }
    }

    public void executeMutation(MetadataEntity entity, Mutation mutation) throws IOException,
            MetadataConflictException
    {
        try {
            Retries.callWithRetries(() -> {
                getDbClient().readWriteTransaction().run(transaction -> { // Use getDbClient() here
                    transaction.buffer(mutation);
                    return null;
                });
                return null;
            }, RetryStrategies.limit(50, RetryStrategies.fixDelay(100,
                    java.util.concurrent.TimeUnit.MILLISECONDS)), e -> e instanceof SpannerException &&
                    ((SpannerException) e).getErrorCode() == ErrorCode.ABORTED);

        } catch (SpannerException e) {
            if (e.getErrorCode() == ErrorCode.ABORTED) {
                // Conflict detected
                throw new MetadataConflictException("Spanner transaction aborted due to conflict", entity);
            } else {
                throw new IOException("Spanner error: " + e.getMessage());
            }
        }
    }

    public List<MetadataChange> batch(List<? extends MetadataMutation> mutations, MutationOptions options) throws
            IOException {
        if (mutations.isEmpty()) {
            return Collections.emptyList();
        }
        if (mutations.size() == 1) {
            return Collections.singletonList(apply(mutations.get(0), options)); // Assuming you have an apply method
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
            List<MetadataChange> changes = new ArrayList<>(mutations.size());
            for (MetadataMutation mutation : mutations) {
                changes.add(apply(mutation, options));
            }
            return changes;
        }

        LinkedHashMap<MetadataEntity, MetadataChange> changes = new LinkedHashMap<>(mutations.size());
        try {
            return Retries.callWithRetries(() -> doBatch(mutationMap, changes, options),
                    RetryStrategies.limit(50, RetryStrategies.fixDelay(100, TimeUnit.MILLISECONDS)),
                    e -> e instanceof SpannerException && ((SpannerException) e).getErrorCode() ==
                            ErrorCode.ABORTED);
        } catch (SpannerException e) {
            if (e.getErrorCode() == ErrorCode.ABORTED) {
                throw new IOException("Spanner transaction aborted due to conflict.");
            } else {
                throw new IOException("Spanner error: " + e.getMessage());
            }
        }
    }

    private List<MetadataChange> doBatch(LinkedHashMap<MetadataEntity, MetadataMutation> mutations,
                                         LinkedHashMap<MetadataEntity, MetadataChange> changes,
                                         MutationOptions options) throws IOException {
        try {
            TransactionRunner runner = getDbClient().readWriteTransaction();
            return runner.run(transaction -> {
                List<MetadataChange> resultChanges = new ArrayList<>();
                for (Map.Entry<MetadataEntity, MetadataMutation> entry : mutations.entrySet()) {
                    MetadataEntity entity = entry.getKey();
                    MetadataMutation mutation = entry.getValue();

                    // 1. Construct Spanner Key from MetadataEntity
                    Key spannerKey = constructSpannerKey(entity);

                    // 2. Read Existing Metadata
                    Struct row = transaction.readRow(METADATA_TABLE, spannerKey, getAllColumns());

                    // 3. Convert Struct to VersionedMetadata
                    VersionedMetadata before = rowToMetadata(row);

                    // 4. Apply Mutation
                    RequestandChange intermediary = applyMutation(before, mutation);
                    LOG.info("applyMutation result for entity: {}, change: {}",entity.toString(), intermediary
                            .getChange().toString());

                    // 5. Convert RequestAndChange to Spanner Mutations
                    List<Mutation> spannerMutations = convertRequestAndChangeToSpannerMutations(intermediary);
                    LOG.info("Spanner mutations generated for entity: {}, mutations: {}", entity.toString(),
                            spannerMutations.toString());

                    // 6. Buffer Spanner Mutations
                    for (Mutation spannerMutation : spannerMutations) {
                        transaction.buffer(spannerMutation);
                        LOG.info("Buffering spanner mutation for entity: {}, mutation: {}", entity.toString(),
                                spannerMutation.toString());
                    }

                    // 7. Store MetadataChange
                    changes.put(entity, intermediary.getChange());
                    resultChanges.add(intermediary.getChange());
                }
                return resultChanges;
            });
        } catch (SpannerException e) {
            if (e.getErrorCode() == com.google.cloud.spanner.ErrorCode.ABORTED) {
                throw e;
            } else {
                LOG.error("Spanner error: {}", e.getMessage(), e);
                throw new IOException("Spanner error: " + e.getMessage());
            }
        }
    }

    private Key constructSpannerKey(MetadataEntity entity) {
        String namespace = entity.getValue("namespace");
        String type = entity.getType();
        String name = entity.getValue(type);

        if (namespace != null && type != null && name != null) {
            return Key.of(namespace, type, name);
        } else {
            LOG.info("No Key formed");
            return null; // Or throw an exception
        }
    }

    private List<Mutation> convertRequestAndChangeToSpannerMutations(RequestandChange requestAndChange) {
        if (requestAndChange == null || requestAndChange.getMutation() == null) {
            return Collections.emptyList();
        }
        return Collections.singletonList(requestAndChange.getMutation()); // Return a list with the mutation
    }

    private List<String> getAllColumns() {
        return Arrays.asList(
                Entity_FIELD, "metadata", NAMESPACE_FIELD, TYPE_FIELD, NAME_FIELD, CREATED_FIELD,
                TTL_FIELD, HIDDEN_FIELD, User_FIELD, SYSTEM_FIELD, "props", "VERSION", pipelineId
        );
    }

    private VersionedMetadata readFromSpanner(TransactionContext transaction, MetadataEntity entity) throws IOException
    {
        try {
            Statement statement = buildSpannerQuery(entity);
            // Debugging print statement.
            LOG.info("Spanner read query: " + statement.getSql());
            try (ResultSet resultSet = transaction.executeQuery(statement)) {
                if (resultSet.next()) {
                    return rowToMetadata(resultSet.getCurrentRowAsStruct());
                } else {
                    return VersionedMetadata.NONE;
                }
            }
        } catch (SpannerException e) {
            LOG.error("Error reading from Spanner: {}", e.getMessage(), e);
            throw new IOException("Error reading from Spanner", e);
        }
    }

    private VersionedMetadata readFromSpanner(MetadataEntity entity) throws IOException {
        try {
            // 1. Construct the Key
            Key key = Key.of(entity.getValue("namespace"), entity.getType(), entity.getValue(entity.getType()));

            // 2. Read the row
            Struct row = getDbClient().singleUse().readRow(METADATA_TABLE, key, Arrays.asList("metadata", "props",
                    "VERSION"));

            // 3. Handle non-existent row
            if (row == null) {
                return VersionedMetadata.NONE;
            }

            // 4. Deserialize metadata and properties
            Type tagType = new TypeToken<Set<ScopedName>>() {}.getType();
            Set<ScopedName> tags = gson.fromJson(row.getString("metadata"), tagType);

            Type propType = new TypeToken<Map<ScopedName, String>>() {}.getType();
            Map<ScopedName, String> properties = gson.fromJson(row.getString("props"), propType);

            Metadata metadata = new Metadata(tags, properties);

            // 5. Construct VersionedMetadata
            long version = row.getLong("VERSION");
            return VersionedMetadata.of(metadata, version);

        } catch (JsonSyntaxException e) {
            LOG.error("Error parsing JSON from Spanner: {}", e.getMessage(), e);
            throw new IOException("Failed to parse JSON from Spanner for entity " + entity, e);
        } catch (Exception e) {
            LOG.error("Failed to read from Spanner for entity {}: {}", entity, e.getMessage(), e);
            throw new IOException("Failed to read from Spanner for entity " + entity, e);
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
        Set<MetadataScope> scopes = meta.getTags().stream().map(ScopedName::getScope).collect(Collectors.toSet());
        scopes.addAll(meta.getProperties().keySet().stream().map(ScopedName::getScope).collect(Collectors.toSet()));

        Set<ScopedName> existingTagsToKeep = new HashSet<>();
        Map<ScopedName, String> existingPropertiesToKeep = new HashMap<>();

        Sets.difference(MetadataScope.ALL, scopes).forEach(scope -> {
            before.getMetadata().getTags().stream().filter(tag -> tag.getScope().equals(scope)).
                    forEach(existingTagsToKeep::add);
            before.getMetadata().getProperties().entrySet().stream().filter(entry ->
                    entry.getKey().getScope().equals(scope)).forEach(entry ->
                    existingPropertiesToKeep.put(entry.getKey(), entry.getValue()));
        });

        directives.entrySet().stream().filter(entry ->
                scopes.contains(entry.getKey().getScope())).forEach(entry -> {
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
                if (existingValue != null && ((entry.getValue() == MetadataDirective.PRESERVE &&
                        !existingValue.equals(newValue)) || (entry.getValue() == MetadataDirective.KEEP &&
                        newValue == null))) {
                    existingPropertiesToKeep.put(property, existingValue);
                }
            }
        });

        Set<ScopedName> newTags = existingTagsToKeep.isEmpty() ? meta.getTags() : Sets.union(meta.getTags(),
                existingTagsToKeep);
        Map<ScopedName, String> newProperties = new HashMap<>(meta.getProperties());
        if (!existingPropertiesToKeep.isEmpty()) {
            newProperties.putAll(existingPropertiesToKeep);
        }

        Metadata after = new Metadata(newTags, newProperties);

        return new RequestandChange(writeToSpanner(create.getEntity(), before.getVersion(), after), new
                MetadataChange(create.getEntity(), before.getMetadata(), after));
    }

    private RequestandChange drop(MetadataEntity entity, VersionedMetadata before) {
        Mutation mutation = deleteFromSpanner(entity, before.getVersion());
        return new RequestandChange(mutation, new MetadataChange(entity, before.getMetadata(), Metadata.EMPTY));
    }

    private RequestandChange update(MetadataEntity entity, VersionedMetadata before, Metadata updates) {
        Set<ScopedName> tags = new HashSet<>(before.getMetadata().getTags());
        tags.addAll(updates.getTags());
        Map<ScopedName, String> properties = new HashMap<>(before.getMetadata().getProperties());
        properties.putAll(updates.getProperties());
        Metadata after = new Metadata(tags, properties);
        return new RequestandChange(writeToSpanner(entity, before.getVersion(), after), new MetadataChange(entity,
                before.getMetadata(), after));
    }


    private Mutation deleteFromSpanner(MetadataEntity entity, long existingVersion) {
        // 1. Construct the key for the row to delete
        Key key = Key.of(entity.getValue("namespace"), entity.getType(), entity.getValue(entity.getType()));

        // 2. Read the row to get the current VERSION
        Struct row = getDbClient().singleUse().readRow(METADATA_TABLE, key, Arrays.asList("VERSION"));

        // 3. Check if the row exists and the VERSION matches
        if (row != null && row.getLong("VERSION") == existingVersion) {
            // 4. If the versions match, create the delete mutation
            return Mutation.delete(METADATA_TABLE, key);
        } else {
            // 5. If the versions don't match or the row doesn't exist, handle the error
            String errorMessage;
            if (row == null) {
                errorMessage = "Row with key " + key + " not found.";
            } else {
                errorMessage = "Version mismatch. Expected version: " + existingVersion +
                        ", Actual version: " + row.getLong("VERSION");
            }
            LOG.warn(errorMessage);
            return null; // Or throw an exception if needed
        }
    }

    private RequestandChange remove(VersionedMetadata before, MetadataMutation.Remove remove) {
        Metadata after = filterMetadata(before.getMetadata(), false, // DISCARD is false
                remove.getKinds(), remove.getScopes(), remove.getRemovals());
        return new RequestandChange(writeToSpanner(remove.getEntity(), before.getVersion(), after),
                new MetadataChange(remove.getEntity(), before.getMetadata(), after));
    }


    private Mutation writeToSpanner(MetadataEntity entity, Long expectVersion, Metadata metadata) {
        // 1. Construct the Key
        if (entity.getValue("namespace") == null || entity.getType() == null || entity.getValue(entity.getType())
                == null) {
            LOG.error("Missing required entity values for Spanner write.");
            return null; // Or throw an exception
        }
        Key key = Key.of(entity.getValue("namespace"), entity.getType(), entity.getValue(entity.getType()));
        String pipelineIdValue = generatePipelineId(entity);

        String metadataJson = gson.toJson(metadata.getTags());
        String propsJson = gson.toJson(metadata.getProperties());

        // 2. Construct the Mutation
        Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(METADATA_TABLE)
                .set("namespace").to(entity.getValue("namespace"))
                .set("type").to(entity.getType())
                .set("name").to(entity.getValue(entity.getType()))
                .set("metadata").to(metadataJson)
                .set("props").to(propsJson)
                .set("pipelineId").to(pipelineIdValue)
                .set("entity").to(entity.toString()); // Added line to set the entity column

        // 3. Handle Versioning (Optimistic Concurrency)
        if (expectVersion == null) {
            // Create or Insert if no version is provided.
            builder.set("VERSION").to(1L);
        } else {
            // Update with version control
            builder.set("VERSION").to(expectVersion + 1);
        }
        LOG.info("Spanner write mutation generated, pipelineId: {}, metadata.getTags(): {}, metadata.getProperties(): "
                + "{}", pipelineIdValue, gson.toJson(metadata.getTags()), gson.toJson(metadata.getProperties()));
        return builder.build();
    }

    private String generatePipelineId(MetadataEntity entity) {
        return entity.getValue("namespace") + "/" + entity.getType() + "/" + entity.getValue(entity.getType());
    }

    public Metadata read(Read read) throws IOException {
        try {
            MetadataEntity entity = read.getEntity();
            Key spannerKey = constructSpannerKey(entity);

            // 1. Read the row from Spanner using singleUse.
            ReadContext readContext = getDbClient().singleUse();
            Struct row = readContext.readRow(METADATA_TABLE, spannerKey, getAllColumns());

            // 2. Convert to MetadataDocument
            SpannerMetadataDocument document = convertStructToMetadataDocument(row);

            // 3. Filter the metadata.
            assert document != null;
            return filterMetadata(document.getMetadata(), KEEP, read.getKinds(), read.getScopes(), read.getSelection());

        } catch (SpannerException e) {
            throw new IOException("Spanner error: " + e.getMessage(), e);
        }
    }

    private SpannerMetadataDocument convertStructToMetadataDocument(Struct row) {
        if (row == null) {
            return null;
        }

        MetadataEntity entity = constructMetadataEntity(row);
        String metadataJson = row.getString("metadata");
        String propsJson = row.getString("props");

        Type tagType = new TypeToken<Set<ScopedName>>() {}.getType();
        Set<ScopedName> tags = gson.fromJson(metadataJson, tagType);

        Type propType = new TypeToken<Map<ScopedName, String>>() {}.getType();
        Map<ScopedName, String> properties = gson.fromJson(propsJson, propType);

        Metadata metadata = new Metadata(tags, properties);

        return SpannerMetadataDocument.of(entity, metadata);
    }

    private MetadataEntity constructMetadataEntity(Struct row) {
        return MetadataEntity.builder()
                .append("namespace", row.getString(NAMESPACE_FIELD))
                .append("type", row.getString(TYPE_FIELD))
                .append("name", row.getString(NAME_FIELD))
                .build();
    }

    public SearchResponse search(SearchRequest request) throws IOException {
        try {
            String sql = buildSearchQuery(request);
            List<MetadataRecord> results = executeSearchQuery(sql, request.getOffset(), request.getLimit());
            int totalResults = getTotalResults(sql); // Get total count
            return new SearchResponse(request, null, request.getOffset(), request.getLimit(), totalResults,
                    results);
        } catch (SpannerException e) {
            throw new IOException("Spanner search failed", e);
        }
    }

    private String buildSearchQuery(SearchRequest request) {
        StringBuilder sql = new StringBuilder("SELECT namespace, type, name, metadata, " +
                "props FROM " +
                METADATA_TABLE + " WHERE 1=1");

        if (request.getScope() != null) {
            sql.append(" AND scope = '").append(request.getScope().name()).append("'");
        }

        if (request.getNamespaces() != null && !request.getNamespaces().isEmpty()) {
            sql.append(" AND namespace IN (");
            addStringListToSql(sql, request.getNamespaces());
            sql.append(")");
        }

        if (request.getTypes() != null && !request.getTypes().isEmpty()) {
            sql.append(" AND type IN (");
            addStringListToSql(sql, request.getTypes());
            sql.append(")");
        }

        if (!request.isShowHidden()) {
            sql.append(" AND name NOT LIKE '_%'");
        }

        if (request.getQuery() != null && !request.getQuery().isEmpty()) {
            sql.append(" AND pipelineId LIKE '%").append(request.getQuery()).append("%'"); // Search pipelineId.
        }

        return sql.toString();
    }

    private void addStringListToSql(StringBuilder sql, Set<String> stringSet) {
        boolean first = true;
        for (String str : stringSet) {
            if (!first) {
                sql.append(", ");
            }
            sql.append("'").append(str).append("'");
            first = false;
        }
    }

    private List<MetadataRecord> executeSearchQuery(String sql, int offset, int limit) {
        List<MetadataRecord> results = new ArrayList<>();
        try (ResultSet resultSet = getDbClient().singleUse().executeQuery(Statement.of(sql + " LIMIT " + limit +
                " OFFSET " + offset))) {
            while (resultSet.next()) {
                MetadataRecord record = createMetadataRecord(resultSet);
                results.add(record);
            }
        }
        return results;
    }


    private MetadataRecord createMetadataRecord(ResultSet resultSet) {
        String namespace = resultSet.getString("namespace");
        String type = resultSet.getString("type");
        String name = resultSet.getString("name");
        String metadataJson = resultSet.getString("metadata");
        String propsJson = resultSet.getString("props");

        Type tagType = new TypeToken<Set<ScopedName>>() {}.getType();
        Set<ScopedName> tags = gson.fromJson(metadataJson, tagType);

        Type propType = new TypeToken<Map<ScopedName, String>>() {}.getType();
        Map<ScopedName, String> properties = gson.fromJson(propsJson, propType);

        Metadata metadata = new Metadata(tags, properties);

        MetadataEntity entity = MetadataEntity.builder()
                .append("namespace", namespace)
                .append("type", type)
                .append("name", name)
                .build();
        return new MetadataRecord(entity, metadata);
    }


    private int getTotalResults(String sql) {
        String countSql = "SELECT COUNT(*) FROM (" + sql + ")";
        try (ResultSet resultSet = getDbClient().singleUse().executeQuery(Statement.of(countSql))) {
            if (resultSet.next()) {
                return (int) resultSet.getLong(0);
            }
        }
        return 0;
    }

    private Statement buildSpannerQuery(MetadataEntity entity) {
        StringBuilder sql = new StringBuilder("SELECT metadata, props, VERSION FROM " + METADATA_TABLE + " WHERE ");
        Iterator<MetadataEntity.KeyValue> iterator = entity.iterator();
        LOG.info(entity.iterator().toString());
        boolean first = true;
        while (iterator.hasNext()) {
            MetadataEntity.KeyValue keyValue = iterator.next();
            LOG.info("Inside the Loop " + keyValue.toString());
            if (!first) {
                sql.append(" AND ");
            }

            String columnName = getString(keyValue);
            sql.append(columnName).append(" = @").append(keyValue.getKey());
            first = false;
        }
        Statement.Builder builder = Statement.newBuilder(sql.toString());
        entity.iterator().forEachRemaining(keyValue -> builder.bind(keyValue.getKey())
                .to(keyValue.getValue()));
        return builder.build();
    }

    private static String getString(MetadataEntity.KeyValue keyValue) {
        String columnName;
        // Map MetadataEntity keys to Spanner column names
        if (keyValue.getKey().equalsIgnoreCase("namespace")) {
            columnName = NAMESPACE_FIELD;
        } else if (keyValue.getKey().equalsIgnoreCase("type")) {
            columnName = TYPE_FIELD;
        } else if (keyValue.getKey().equalsIgnoreCase("name")) {
            columnName = NAME_FIELD;
        } else {
            // Handle other keys based on your logic, if necessary.
            columnName = keyValue.getKey(); // Default to the key itself
        }
        return columnName;
    }


    private VersionedMetadata rowToMetadata(Struct row) {
        if (row == null) {
            return VersionedMetadata.NONE;
        }

        String metadataJson = row.getString("metadata");
        String propsJson = row.getString("props");
        Long version = row.getLong("VERSION");

        Type tagType = new TypeToken<Set<ScopedName>>() {}.getType();
        Set<ScopedName> tags;
        try {
            tags = gson.fromJson(metadataJson, tagType);
        } catch (JsonSyntaxException e) {
            LOG.error("Error parsing metadata JSON: {}", metadataJson, e);
            tags = Collections.emptySet(); // Or handle the error as appropriate
        }

        Type propType = new TypeToken<Map<ScopedName, String>>() {}.getType();
        Map<ScopedName, String> properties;
        try {
            properties = gson.fromJson(propsJson, propType);
        } catch (JsonSyntaxException e) {
            LOG.error("Error parsing props JSON: {}", propsJson, e);
            properties = Collections.emptyMap(); // Or handle the error as appropriate
        }

        Metadata metadata = new Metadata(tags, properties);

        return VersionedMetadata.of(metadata, version);
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

    static Metadata filterMetadata(Metadata metadata, boolean keep, Set<MetadataKind> kinds,
                                   Set<MetadataScope> scopes, Set<ScopedNameOfKind> selection) {
        if (selection != null) {
            return new Metadata(
                    metadata.getTags().stream()
                            .filter(tag -> keep == selection.contains(
                                    new ScopedNameOfKind(MetadataKind.TAG, tag.getScope(), tag.getName())))
                            .collect(Collectors.toSet()),
                    metadata.getProperties().entrySet().stream()
                            .filter(entry -> keep == selection.contains(
                                    new ScopedNameOfKind(MetadataKind.PROPERTY, entry.getKey().getScope(),
                                            entry.getKey().getName())))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );
        }
        return new Metadata(
                metadata.getTags().stream()
                        .filter(tag -> keep == (kinds.contains(MetadataKind.TAG) && scopes.
                                contains(tag.getScope())))
                        .collect(Collectors.toSet()),
                metadata.getProperties().entrySet().stream()
                        .filter(entry -> keep == (kinds.contains(MetadataKind.PROPERTY) &&
                                scopes.contains(entry.getKey().getScope())))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
        );
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

    @Override
    public Object getDatasetMetadata(String datasetName) {
        return Collections.emptyMap();
    }

}