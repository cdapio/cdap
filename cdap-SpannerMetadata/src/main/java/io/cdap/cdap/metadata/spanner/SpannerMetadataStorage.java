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
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ErrorCode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.metadata.Cursor;
import io.cdap.cdap.common.metadata.MetadataConflictException;
import io.cdap.cdap.common.metadata.MetadataUtil;
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
import io.cdap.cdap.spi.metadata.VersionedMetadata;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nullable;
import java.lang.reflect.Type;
import java.util.Set;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static io.cdap.cdap.spi.metadata.MetadataConstants.KEYVALUE_SEPARATOR;

/**
 * SpannerMetadataStorage implements the MetadataStorage interface
 * using Google Cloud Spanner as the underlying storage.
 * It provides methods for managing metadata within a Spanner database.
 */
public class SpannerMetadataStorage implements MetadataStorage {


    private static final Logger LOG = LoggerFactory.getLogger(SpannerMetadataStorage.class);

    private static final String METADATA_TABLE = "metadata"; // Table name
    private static final String METADATA_PROPS_TABLE = "metadata_props"; // Table name
    private static final String NAMESPACE_FIELD = "Namespace"; // Namespace of the metadata
    private static final String TYPE_FIELD = "Entity_type"; // Type of the entity (e.g., dataset, application)
    private static final String NAME_FIELD = "Name"; // Name of the entity
    private static final String CREATED_FIELD = "Creation_Time"; // Creation timestamp
    private static final String TTL_FIELD = "TTL"; // Time-to-live
    private static final String HIDDEN_FIELD = "Hidden"; // Hidden flag
    private static final String User_FIELD = "USER"; // User-scoped text data
    private static final String SYSTEM_FIELD = "SYSTEM"; // System-scoped text data
    private static final String NESTED_NAME_FIELD = "Props_Name"; // contains the property name in nested props
    private static final String NESTED_SCOPE_FIELD = "Props_Scope"; // contains the scope in nested props
    private static final String NESTED_VALUE_FIELD = "Props_Value"; // contains the value in nested props

    private String instanceId = "instance";
    private String databaseId = "database";
    private String projectId = "project";
    private volatile SpannerOptions options;
    private volatile Spanner spanner;
    private volatile DatabaseClient dbClient;
    private volatile DatabaseAdminClient adminClient;
    private volatile boolean created;
    private static final String CREDENTIALS_PATH = "credentials.path";

    @VisibleForTesting
    static final boolean KEEP = true;
    @VisibleForTesting
    static final boolean DISCARD = false;

    private static final Pattern SPACE_SEPARATOR_PATTERN = Pattern.compile("\\s+");


    private static final Gson gson = new GsonBuilder()
            .registerTypeAdapter(ScopedName.class, new ScopedNameTypeAdapter())
            .registerTypeAdapter(ScopedNameOfKind.class, new ScopedNameOfKindTypeAdapter())
            .create();


    @Override
    public void initialize(MetadataStorageContext context) throws Exception {
        LOG.info("Initializing SpannerMetadataStorage...");
        Map<String, String> conf = context.getConfiguration();
        LOG.info(conf.toString());
        projectId = getProjectId(conf);
        databaseId = getDatabaseId(conf);
        instanceId = getInstanceId(conf);
        LOG.info(projectId, databaseId, instanceId);
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
        LOG.info("Creating Table for SpannerMetadataStorage...");
        if (created) {
            LOG.info("Table already created.");
            return;
        }
        synchronized (this) {
            if (created) {
                LOG.info("Table already created (within sync block).");
                return;
            }
            try {
                if (!tableExists(METADATA_TABLE) && !tableExists(METADATA_PROPS_TABLE)) { // Check if the table exists
                    createMetadataTable();
                    LOG.info("metadata and metadata_props table created successfully in Spanner database '{}'",
                            databaseId);
                } else {
                    LOG.info("metadata and metadata_props table already exists in Spanner database '{}'", databaseId);
                }

                // Perform a read operation to ensure the table is ready
                //readFromSpanner(MetadataEntity.ofNamespace("system"));

                created = true;
                LOG.debug("Spanner index {} is ready to use.", METADATA_TABLE);

            } catch (SpannerException | InterruptedException | ExecutionException e) {
                LOG.error("Error creating or verifying Metadata table in Spanner database '{}': {}",
                        databaseId, e.getMessage(), e);
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new IOException("Error creating or verifying metadata and metadata_props table: " +
                        e.getMessage(), e);
            }
        }
        LOG.info("Index creation completed.");
    }

    private boolean tableExists(String tableNameToCheck) {
        try {
            String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @tableName";
            Statement statement = Statement.newBuilder(sql)
                    .bind("tableName").to(tableNameToCheck)
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
        ddlStatements.add(MetadataTableDDLStatement());
        ddlStatements.add(Metadata_propsTableDDLStatement());
        ddlStatements.add(getUSERSearchIndexDDLStatement());
        ddlStatements.add(getSYSTEMSearchIndexDDLStatement());
        ddlStatements.add(getTEXTSearchIndexDDLStatement());
        ddlStatements.add(getValueSearchIndexDDLStatement());
        executeCreateDDLStatements(ddlStatements);
        LOG.info("Metadata table creation completed.");
    }

    private String MetadataTableDDLStatement() {
        LOG.info("Generating create table DDL statement.");
        return String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        "metadata_id STRING(MAX) NOT NULL," +
                        "%s STRING(MAX) NOT NULL," + // namespace
                        "%s STRING(MAX) NOT NULL," + // entity_type
                        "%s STRING(MAX) NOT NULL," + // name
                        "%s INT64," + // created
                        "%s INT64," + // ttl
                        "%s BOOL," + // hidden
                        "%s STRING(MAX)," + // user
                        "%s STRING(MAX)," + // system
                        "metadata_column JSON," + // metadata
                        "VERSION INT64 NOT NULL," +
                        "User_Tokens TOKENLIST AS " +
                        "(TOKENIZE_SUBSTRING(USER, ngram_size_min=>1, ngram_size_max=>3)) HIDDEN," +
                        "System_Tokens TOKENLIST AS " +
                        "(TOKENIZE_SUBSTRING(SYSTEM, ngram_size_min=>1, ngram_size_max=>3)) HIDDEN," +
                        "Text_Tokens TOKENLIST AS " +
                        "(TOKENLIST_CONCAT([User_Tokens, System_Tokens])) HIDDEN," +
                        ")PRIMARY KEY (metadata_id) ", // primary key
                METADATA_TABLE,
                NAMESPACE_FIELD,
                TYPE_FIELD,
                NAME_FIELD,
                CREATED_FIELD,
                TTL_FIELD,
                HIDDEN_FIELD,
                User_FIELD,
                SYSTEM_FIELD
        );
    }

    private String Metadata_propsTableDDLStatement() {
        LOG.info("Generating Metadata_props table DDL statement.");
        return String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        "metadata_id STRING(MAX) NOT NULL," +
                        "%s STRING(MAX) NOT NULL," + // name
                        "%s STRING(MAX)," + // scope
                        "%s STRING(MAX)," + // value
                        "Value_Tokens TOKENLIST AS " +
                        "(TOKENIZE_SUBSTRING(Props_Value, ngram_size_min=>1, ngram_size_max=>3)) HIDDEN," +
                        ")PRIMARY KEY (metadata_id, %s, %s) ," + // primary key
                        "INTERLEAVE IN PARENT metadata ON DELETE CASCADE",
                METADATA_PROPS_TABLE,
                NESTED_NAME_FIELD,
                NESTED_SCOPE_FIELD,
                NESTED_VALUE_FIELD,
                NESTED_NAME_FIELD,
                NESTED_SCOPE_FIELD
        );
    }

    private String getUSERSearchIndexDDLStatement() {
        return String.format("CREATE SEARCH INDEX USERNgramIndex ON %s(User_Tokens)", METADATA_TABLE);
    }

    private String getSYSTEMSearchIndexDDLStatement() {
        return String.format("CREATE SEARCH INDEX SYSTEMNgramIndex ON %s(System_Tokens)", METADATA_TABLE);
    }

    private String getTEXTSearchIndexDDLStatement() {
        return String.format("CREATE SEARCH INDEX TEXTNgramIndex ON %s(Text_Tokens)", METADATA_TABLE);
    }


    private String getValueSearchIndexDDLStatement() {
        return String.format("CREATE SEARCH INDEX ValueNgramIndex ON %s(Value_Tokens)", METADATA_PROPS_TABLE);
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
            TransactionRunner runner = dbClient.readWriteTransaction();
            return runner.run(transaction -> {
                VersionedMetadata before = readFromSpanner(entity, transaction); // Use transaction directly
                assert before != null;
                RequestandChange intermediary = applyMutation(before, mutation);
                executeMutation(transaction, mutation.getEntity(), intermediary.getMutation(), options);
                return intermediary.getChange();
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
    private void executeMutation(TransactionContext transaction, MetadataEntity entity, List<Mutation> mutations,
                                 MutationOptions options) throws IOException {
        try {
            for (Mutation mutation : mutations) { // Iterate over the list of mutations
                if (mutation.getOperation() == Mutation.Op.INSERT || mutation.getOperation() == Mutation.Op.UPDATE ||
                        mutation.getOperation() == Mutation.Op.INSERT_OR_UPDATE) {
                    transaction.buffer(mutation);
                } else if (mutation.getOperation() == Mutation.Op.DELETE) {
                    transaction.buffer(mutation);
                } else {
                    throw new IllegalStateException("Unexpected Mutation operation: " + mutation.getOperation());
                }
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

    private MetadataChange createMetadataChange(MetadataMutation mutation, Metadata existingMetadata,
                                                Metadata updatedMetadata) {
        return new MetadataChange(mutation.getEntity(), existingMetadata, updatedMetadata);
    }

    /**
     * Reads the existing metadata for an entity from the index.
     *
     * @return existing metadata along with its version in the index, or an empty metadata with null
     *     version.
     */

    public VersionedMetadata readFromSpanner(MetadataEntity entity, TransactionContext transaction) throws IOException {
        try {
            Statement statement = Statement.newBuilder(
                "SELECT metadata_column, VERSION FROM metadata " +
                  "WHERE Namespace = @namespace AND Entity_type = @Type AND Name = @name")
              .bind("namespace").to(entity.getValue(MetadataEntity.NAMESPACE))
              .bind("Type").to(entity.getType())
              .bind("name").to(entity.getValue(entity.getType()))
              .build();

            ResultSet resultSet = transaction.executeQuery(statement);

            if (resultSet.next()) {
                Struct row = resultSet.getCurrentRowAsStruct();
                LOG.info("Row Struct: {}", row);

                // Get the JSON string from the Spanner JSON column
                String metadataString = row.getJson(0).toString(); // Convert Spanner JSON to String

                long version = row.getLong(1);

                // --- IMPORTANT: Use the configured 'gson' instance for direct deserialization ---
                Metadata metadata = gson.fromJson(metadataString, Metadata.class);

                return VersionedMetadata.of(metadata, version);
            } else {
                return VersionedMetadata.NONE;
            }
        } catch (SpannerException | JsonParseException e) { // Add JsonParseException to catch possible issues
            LOG.error("Spanner exception or JSON parsing error during read:", e);
            throw new IOException("Failed to read from Spanner for entity " + entity, e);
        }
    }

    public VersionedMetadata readFromSpanner(MetadataEntity entity, ReadOnlyTransaction transaction) throws
      IOException {
        try {
            Statement statement = Statement.newBuilder(
                "SELECT metadata_column, VERSION FROM metadata " +
                  "WHERE Namespace = @namespace AND Entity_type = @Type AND Name = @name")
              .bind("namespace").to(entity.getValue(MetadataEntity.NAMESPACE))
              .bind("Type").to(entity.getType())
              .bind("name").to(entity.getValue(entity.getType()))
              .build();

            ResultSet resultSet = transaction.executeQuery(statement);

            if (resultSet.next()) {
                Struct row = resultSet.getCurrentRowAsStruct();
                LOG.info("Row Struct: {}", row);

                // Get the JSON string from the Spanner JSON column
                String metadataString = row.getJson(0).toString(); // Convert Spanner JSON to String

                long version = row.getLong(1);

                // --- IMPORTANT: Use the configured 'gson' instance for direct deserialization ---
                Metadata metadata = gson.fromJson(metadataString, Metadata.class);

                return VersionedMetadata.of(metadata, version);
            } else {
                return VersionedMetadata.NONE;
            }
        } catch (SpannerException | JsonParseException e) { // Add JsonParseException to catch possible issues
            LOG.error("Spanner exception or JSON parsing error during read:", e);
            throw new IOException("Failed to read from Spanner for entity " + entity, e);
        }
    }

    /**
     * Creates a Spanner request that corresponds to the given mutation, along with the change
     * effected by this mutation. The request must be executed by the caller.
     *
     * @param before the metadata for the mutation's entity before the change
     * @param mutation the mutation to apply
     * @return an ElasticSearch request to be executed, and the change caused by the mutation.
     */

    private RequestandChange applyMutation(VersionedMetadata before, MetadataMutation mutation) throws IOException {
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

    /**
     * Creates the Spanner request for an entity creation or update. See {@link
     * MetadataMutation.Create} for detailed semantics.
     *
     * @param before the metadata for the mutation's entity before the change
     * @param create the mutation to apply
     * @return an ElasticSearch request to be executed, and the change caused by the mutation
     */
    private RequestandChange create(VersionedMetadata before, MetadataMutation.Create create) throws IOException {
        // if the entity did not exist before, none of the directives apply and this is equivalent to update()
        Metadata meta = create.getMetadata();
        Map<ScopedNameOfKind, MetadataDirective> directives = create.getDirectives();
        // determine the scopes that this mutation applies to (scopes that do not occur in the metadata are no changed)
        Set<MetadataScope> scopes = Stream.concat(meta.getTags().stream(),
                        meta.getProperties().keySet().stream())
                .map(ScopedName::getScope).collect(Collectors.toSet());
        // compute what previously existing tags and properties have to be preserved (all others are replaced)
        Set<ScopedName> existingTagsToKeep = new HashSet<>();
        Map<ScopedName, String> existingPropertiesToKeep = new HashMap<>();
        // all tags and properties that are in a scope not affected by this mutation
        Sets.difference(MetadataScope.ALL, scopes).forEach(
                scope -> {
                    before.getMetadata().getTags().stream()
                            .filter(tag -> tag.getScope().equals(scope))
                            .forEach(existingTagsToKeep::add);
                    before.getMetadata().getProperties().entrySet().stream()
                            .filter(entry -> entry.getKey().getScope().equals(scope))
                            .forEach(entry -> existingPropertiesToKeep.put(entry.getKey(),
                                    entry.getValue()));
                });
        // tags and properties in affected scopes that must be kept or preserved
        directives.entrySet().stream()
                .filter(entry -> scopes.contains(entry.getKey().getScope()))
                .forEach(entry -> {
                    ScopedNameOfKind key = entry.getKey();
                    if (key.getKind() == MetadataKind.TAG
                            && (entry.getValue() == MetadataDirective.PRESERVE
                            || entry.getValue() == MetadataDirective.KEEP)) {
                        ScopedName tag = new ScopedName(key.getScope(), key.getName());
                        if (!meta.getTags().contains(tag) && before.getMetadata().getTags().contains(tag)) {
                            existingTagsToKeep.add(tag);
                        }
                    } else if (key.getKind() == MetadataKind.PROPERTY) {
                        ScopedName property = new ScopedName(key.getScope(), key.getName());
                        String existingValue = before.getMetadata().getProperties().get(property);
                        String newValue = meta.getProperties().get(property);
                        if (existingValue != null
                                && (
                                entry.getValue() == MetadataDirective.PRESERVE && !existingValue.equals(newValue)
                                        || entry.getValue() == MetadataDirective.KEEP && newValue == null)) {
                            existingPropertiesToKeep.put(property, existingValue);
                        }
                    }
                });
        // compute the new tags and properties
        Set<ScopedName> newTags =
                existingTagsToKeep.isEmpty() ? meta.getTags()
                        : Sets.union(meta.getTags(), existingTagsToKeep);
        Map<ScopedName, String> newProperties = meta.getProperties();
        if (!existingPropertiesToKeep.isEmpty()) {
            newProperties = new HashMap<>(newProperties);
            newProperties.putAll(existingPropertiesToKeep);
        }
        Metadata after = new Metadata(newTags, newProperties);
        return new RequestandChange(writeToSpanner(create.getEntity(), before.getVersion(), after),
                new MetadataChange(create.getEntity(), before.getMetadata(), after));
    }

    /**
     * Creates the Spanner delete request for an entity deletion. This drops the corresponding
     * metadata document from the index.
     *
     * @param before the metadata for the mutation's entity before the change
     * @return an ElasticSearch request to be executed, and the change caused by the mutation
     */

    private RequestandChange drop(MetadataEntity entity, VersionedMetadata before) {
        return new RequestandChange(deleteFromSpanner(entity, before.getVersion()),
                new MetadataChange(entity, before.getMetadata(), Metadata.EMPTY));
    }

    /**
     * Create a Spanner Delete Request for removing an row in the table. The request must be
     * executed by the caller.
     */
    private List<Mutation> deleteFromSpanner(MetadataEntity entity, Long existingVersion) {
        List<Mutation> mutations = new ArrayList<>();

        String metadataId = toDocumentId(entity); // Get the metadata_id for the entity

        if (existingVersion == null) {
            throw new IllegalArgumentException("existingVersion cannot be null when deleting a " +
                    "specific version of a metadata entity.");
        }

        // --- Delete from Metadata table ---
        // The primary key for Metadata table is (metadata_id, VERSION)
        mutations.add(Mutation.delete(
                "Metadata", // Your parent table name
                Key.of(metadataId, existingVersion) // Provide both components of the primary key
        ));
        return mutations;
    }


    /**
     * Creates the Spanner request for updating the metadata of an entity. This updates or
     * adds the new metadata.
     *
     * @param before the metadata for the mutation's entity before the change, and its version
     * @return an ElasticSearch request to be executed, and the change caused by the mutation
     */
    private RequestandChange update(MetadataEntity entity,
                                    VersionedMetadata before,
                                    Metadata updates) {
        Set<ScopedName> tags = new HashSet<>(before.getMetadata().getTags());
        tags.addAll(updates.getTags());
        Map<ScopedName, String> properties = new HashMap<>(before.getMetadata().getProperties());
        properties.putAll(updates.getProperties());
        Metadata after = new Metadata(tags, properties);
        try {
            return new RequestandChange(writeToSpanner(entity, before.getVersion(), after),
                    new MetadataChange(entity, before.getMetadata(), after));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates the Spanner index request for removing some metadata from an entity. This removed
     * the specified metadata from the corresponding metadata document in the index.
     *
     * Note that even if all tags and properties are removed, the document will remain in the index
     * and it still searchable by its type, name, or a match-all query. Use {@link
     * MetadataMutation.Drop} to completely remove the entity from the index.
     *
     * @param before the metadata for the mutation's entity before the change
     * @return an ElasticSearch request to be executed, and the change caused by the mutation
     */

    private RequestandChange remove(VersionedMetadata before, MetadataMutation.Remove remove) throws IOException {
        Metadata after = filterMetadata(before.getMetadata(), DISCARD,
                remove.getKinds(), remove.getScopes(), remove.getRemovals());
        return new RequestandChange(writeToSpanner(remove.getEntity(), before.getVersion(), after),
                new MetadataChange(remove.getEntity(), before.getMetadata(), after));
    }

    public List<Mutation> writeToSpanner(MetadataEntity entity, Long expectVersion, Metadata metadata) throws
            IOException {
        List<Mutation> mutations = new ArrayList<>();

        // --- Parent Metadata Table Mutation ---
        // The parent table still uses 'VERSION' as part of its primary key or for optimistic concurrency
        Mutation.WriteBuilder parentMutationBuilder = Mutation.newInsertOrUpdateBuilder("Metadata")
                .set("metadata_id").to(toDocumentId(entity))
                .set("Namespace").to(entity.getValue(MetadataEntity.NAMESPACE))
                .set("Entity_type").to(entity.getType())
                .set("Name").to(entity.getValue(entity.getType()))
                .set("metadata_column").to(gson.toJson(metadata)) // Still store full metadata JSON if needed
                .set("VERSION").to(expectVersion == null ? 1L : expectVersion + 1);

        Map<String, String> systemProperties = metadata.getProperties(MetadataScope.SYSTEM);
        Map<String, String> userProperties = metadata.getProperties(MetadataScope.USER);
        Set<String> userTags = metadata.getTags(MetadataScope.USER);
        Set<String> systemTags = metadata.getTags(MetadataScope.SYSTEM);

        // StringBuilders for 'user', 'system' columns in the parent Metadata table
        StringBuilder userStringBuilder = new StringBuilder();
        StringBuilder systemStringBuilder = new StringBuilder();

        // The parent's metadata_id will be used as the interleaving key for child mutations
        String parentMetadataId = toDocumentId(entity);
        long parentVersion = expectVersion == null ? 1L : expectVersion + 1; // The version of the parent record

        // --- Prepare data for metadata_props (interleaved table) ---

        // User tags for metadata_props
        if (!userTags.isEmpty()) {
            Mutation childTagMutation = Mutation.newInsertOrUpdateBuilder(METADATA_PROPS_TABLE)
                    .set("metadata_id").to(parentMetadataId) // Parent's primary key (interleaving key)
                    .set(NESTED_NAME_FIELD).to("tags") // props_name
                    .set(NESTED_SCOPE_FIELD).to(MetadataScope.USER.name()) // props_scope
                    .set(NESTED_VALUE_FIELD).to(String.join(" ", userTags).toLowerCase()) // props_value
                    .build();
            mutations.add(childTagMutation);
        }
        // User properties (individual entries) for metadata_props
        for (Map.Entry<String, String> entry : userProperties.entrySet()) {
            Mutation childPropMutation = Mutation.newInsertOrUpdateBuilder(METADATA_PROPS_TABLE)
                    .set("metadata_id").to(parentMetadataId) // Parent's primary key
                    .set(NESTED_NAME_FIELD).to(entry.getKey().toLowerCase()) // props_name
                    .set(NESTED_SCOPE_FIELD).to(MetadataScope.USER.name()) // props_scope
                    .set(NESTED_VALUE_FIELD).to(entry.getValue().toLowerCase()) // props_value
                    .build();
            mutations.add(childPropMutation);
        }
        // User properties summary for metadata_props (if desired as a separate entry)
        if (!userProperties.isEmpty()) {
            Mutation childPropSummaryMutation = Mutation.newInsertOrUpdateBuilder(METADATA_PROPS_TABLE)
                    .set("metadata_id").to(parentMetadataId) // Parent's primary key
                    .set(NESTED_NAME_FIELD).to("properties") // props_name
                    .set(NESTED_SCOPE_FIELD).to(MetadataScope.USER.name()) // props_scope
                    .set(NESTED_VALUE_FIELD).to(String.join(" ", userProperties.keySet()).toLowerCase())
                    // props_value
                    .build();
            mutations.add(childPropSummaryMutation);
        }

        // --- Handle SYSTEM scope ---
        // System tags for metadata_props
        if (!systemTags.isEmpty()) {
            Mutation childTagMutation = Mutation.newInsertOrUpdateBuilder(METADATA_PROPS_TABLE)
                    .set("metadata_id").to(parentMetadataId) // Parent's primary key
                    .set(NESTED_NAME_FIELD).to("tags") // props_name
                    .set(NESTED_SCOPE_FIELD).to(MetadataScope.SYSTEM.name()) // props_scope
                    .set(NESTED_VALUE_FIELD).to(String.join(" ", systemTags).toLowerCase()) // props_value
                    .build();
            mutations.add(childTagMutation);
        }
        // System properties (individual entries) for metadata_props
        for (Map.Entry<String, String> entry : systemProperties.entrySet()) {
            String key = entry.getKey().toLowerCase();
            String value = entry.getValue(); // Use original value for complex types like schema
            Mutation childPropMutation = Mutation.newInsertOrUpdateBuilder(METADATA_PROPS_TABLE)
                    .set("metadata_id").to(parentMetadataId) // Parent's primary key
                    .set(NESTED_NAME_FIELD).to(key) // props_name
                    .set(NESTED_SCOPE_FIELD).to(MetadataScope.SYSTEM.name()) // props_scope
                    .set(NESTED_VALUE_FIELD).to(value) // props_value
                    .build();
            mutations.add(childPropMutation);

            // Handle special case for schema value extraction for additional props_name entries
            if (key.equals("schema")) {
                try {
                    JsonObject schemaJson = gson.fromJson(value, JsonObject.class);
                    JsonElement nameElement = schemaJson.get("name");
                    if (nameElement != null && nameElement.isJsonPrimitive()) {
                        Mutation schemaNamePropMutation = Mutation.newInsertOrUpdateBuilder(METADATA_PROPS_TABLE)
                                .set("metadata_id").to(parentMetadataId)
                                .set(NESTED_NAME_FIELD).to("schema_name") // props_name for schema name
                                .set(NESTED_SCOPE_FIELD).to(MetadataScope.SYSTEM.name()) // props_scope
                                .set(NESTED_VALUE_FIELD).to(nameElement.getAsString().toLowerCase())
                                .build();
                        mutations.add(schemaNamePropMutation);
                    }
                    // Basic extraction of field names and types (first level)
                    JsonArray fields = schemaJson.getAsJsonArray("fields");
                    if (fields != null) {
                        StringBuilder schemaFieldsValue = new StringBuilder();
                        for (JsonElement fieldElement : fields) {
                            if (fieldElement.isJsonObject()) {
                                JsonObject fieldObject = fieldElement.getAsJsonObject();
                                JsonElement fieldNameElement = fieldObject.get("name");
                                JsonElement fieldTypeElement = fieldObject.get("type");
                                if (fieldNameElement != null && fieldNameElement.isJsonPrimitive() &&
                                        fieldTypeElement != null && fieldTypeElement.isJsonPrimitive()) {
                                    schemaFieldsValue.append(fieldNameElement.getAsString().toLowerCase()).append(":")
                                            .append(fieldTypeElement.getAsString().toLowerCase()).append(" ");
                                }
                            }
                        }
                        if (schemaFieldsValue.length() > 0) {
                            Mutation schemaFieldsPropMutation = Mutation.newInsertOrUpdateBuilder(METADATA_PROPS_TABLE)
                                    .set("metadata_id").to(parentMetadataId)
                                    .set(NESTED_NAME_FIELD).to("schema_fields") // props_name for schema fields
                                    .set(NESTED_SCOPE_FIELD).to(MetadataScope.SYSTEM.name()) // props_scope
                                    .set(NESTED_VALUE_FIELD).to(schemaFieldsValue.toString().trim()) // props_value
                                    .build();
                            mutations.add(schemaFieldsPropMutation);
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Error parsing schema JSON for props: " + e.getMessage());
                }
            }
        }
        // System properties summary for metadata_props (if desired as a separate entry)
        if (!systemProperties.isEmpty()) {
            Mutation childPropSummaryMutation = Mutation.newInsertOrUpdateBuilder(METADATA_PROPS_TABLE)
                    .set("metadata_id").to(parentMetadataId) // Parent's primary key
                    .set(NESTED_NAME_FIELD).to("properties_summary") // props_name for summary
                    .set(NESTED_SCOPE_FIELD).to(MetadataScope.SYSTEM.name()) // props_scope
                    .set(NESTED_VALUE_FIELD).to(String.join(" ", systemProperties.keySet()).toLowerCase())
                    // props_value
                    .build();
            mutations.add(childPropSummaryMutation);
        }

        // --- Handle Entity Information for metadata_props ---
        // Entity name
        if (entity.getValue(entity.getType()) != null) {
            Mutation entityNamePropMutation = Mutation.newInsertOrUpdateBuilder(METADATA_PROPS_TABLE)
                    .set("metadata_id").to(parentMetadataId) // Parent's primary key
                    .set(NESTED_NAME_FIELD).to("entity-name") // props_name
                    .set(NESTED_SCOPE_FIELD).to(MetadataScope.SYSTEM.name()) // props_scope
                    .set(NESTED_VALUE_FIELD).to(entity.getValue(entity.getType()).toLowerCase()) // props_value
                    .build();
            mutations.add(entityNamePropMutation);
        }

        // --- Parent Metadata Table updates for 'user', 'system', 'text' columns ---
        // These still populate the parent table's columns for full-text search strings,
        // as they are typically used for broader textual search across metadata.
        String createdString = systemProperties.get("creation-time"); // From SYSTEM properties

        // Populate 'user' column
        if (!userTags.isEmpty()) {
            userStringBuilder.append(String.join(" ", userTags).toLowerCase()).append(" ");
        }
        if (!userProperties.isEmpty()) {
            for (Map.Entry<String, String> entry : userProperties.entrySet()) {
                userStringBuilder.append(entry.getKey().toLowerCase()).append(":").
                        append(entry.getValue().toLowerCase()).append(" ");
            }
        }
        parentMutationBuilder.set("user").to(userStringBuilder.toString().trim());

        // Populate 'system' column
        String entityType = systemProperties.get("type");
        if (entityType != null) {
            systemStringBuilder.append(entityType.toLowerCase()).append(" ");
        }
        String entityNameFromSystemProps = systemProperties.get("entity-name");
        if (entityNameFromSystemProps != null) {
            systemStringBuilder.append(entityNameFromSystemProps.toLowerCase()).append(" ").
                    append(entityNameFromSystemProps.toLowerCase()).append(" ");
        }
        if (!systemTags.isEmpty()) {
            systemStringBuilder.append(String.join(" ", systemTags).toLowerCase()).append(" ");
        }
        if (createdString != null) {
            systemStringBuilder.append(createdString).append(" ");
        }
        String schema = systemProperties.get("schema");
        if (schema != null) {
            try {
                JsonObject schemaJson = gson.fromJson(schema, JsonObject.class);
                JsonElement schemaNameElement = schemaJson.get("name");
                if (schemaNameElement != null && schemaNameElement.isJsonPrimitive()) {
                    systemStringBuilder.append(schemaNameElement.getAsString().toLowerCase()).append(" ");
                }
                JsonArray fields = schemaJson.getAsJsonArray("fields");
                if (fields != null) {
                    for (JsonElement fieldElement : fields) {
                        if (fieldElement.isJsonObject()) {
                            JsonObject fieldObject = fieldElement.getAsJsonObject();
                            JsonElement nameElement = fieldObject.get("name");
                            JsonElement typeElement = fieldObject.get("type");
                            if (typeElement != null && typeElement.isJsonPrimitive()) {
                                systemStringBuilder.append(nameElement.getAsString().toLowerCase()).append(":")
                                        .append(typeElement.getAsString().toLowerCase()).append(" ");
                            }
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error parsing schema JSON for system string: " + e.getMessage());
            }
        }
        parentMutationBuilder.set("system").to(systemStringBuilder.toString().trim());


        // --- Add parent mutation to the list of mutations ---
        // It's good practice to add the parent mutation first, although Spanner handles atomicity.
        mutations.add(0, parentMutationBuilder.build());

        return mutations;
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

        // Spanner Batch Processing (No Duplicates) with Retry
        return retrySpannerBatch(mutationMap, options);
    }


    private List<MetadataChange> retrySpannerBatch(LinkedHashMap<MetadataEntity, MetadataMutation> mutations,
                                                   MutationOptions options) throws IOException {
        int maxRetries = 50; // Set a maximum number of retries
        int retryDelayMillis = 100; // Set an initial retry delay

        for (int retryCount = 0; retryCount < maxRetries; retryCount++) {
            try {
                return doSpannerBatch(mutations);
            } catch (SpannerException e) {
                if (e.getErrorCode() == ErrorCode.ABORTED) {
                    LOG.warn("Spanner transaction aborted, retrying (attempt {}/{}): {}", retryCount + 1,
                            maxRetries, e.getMessage());
                    try {
                        Thread.sleep(retryDelayMillis);
                        // Exponential backoff with a maximum delay of 5 seconds
                        retryDelayMillis = Math.min(retryDelayMillis * 2, 5000);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt(); // Restore interrupted state.
                        throw new IOException("Retry interrupted", ex);
                    }
                } else {
                    LOG.error("Spanner exception during batch: {}", e.getMessage(), e);
                    throw new IOException("Spanner exception during batch", e);
                }
            }
        }

        throw new MetadataConflictException("Spanner batch failed after " + maxRetries + " retries",
                new ArrayList<>(mutations.keySet()));
    }


    private List<MetadataChange> doSpannerBatch(LinkedHashMap<MetadataEntity, MetadataMutation> mutations
                                                ) throws IOException {
        List<MetadataChange> changes = new ArrayList<>(mutations.size());
        try {
            TransactionRunner runner = dbClient.readWriteTransaction();
            runner.run(transaction -> {
                for (MetadataMutation mutation : mutations.values()) {
                    MetadataEntity entity = mutation.getEntity();

                    // Use the existing read-write transaction for read operations
                    VersionedMetadata existingMetadata = readFromSpanner(entity, transaction);

                    Metadata updatedMetadata;
                    if (existingMetadata != null) {
                        RequestandChange requestAndChange = applyMutation(existingMetadata, mutation);
                        updatedMetadata = requestAndChange.getChange().getAfter();
                    } else {
                        if (mutation instanceof MetadataMutation.Create) {
                            updatedMetadata = ((MetadataMutation.Create) mutation).getMetadata();
                            existingMetadata = VersionedMetadata.of(Metadata.EMPTY, System.currentTimeMillis());
                        } else {
                            LOG.warn("Metadata not found, skipping mutation of type: {}", mutation.getType());
                            continue;
                        }
                    }

                    transaction.buffer(writeToSpanner(entity, existingMetadata.getVersion(), updatedMetadata));
                    changes.add(createMetadataChange(mutation, existingMetadata.getMetadata(), updatedMetadata));
                }
                return null;
            });
        } catch (SpannerException e) {
            throw e;
        }
        return changes;
    }

    @Override
    public Metadata read(Read read) throws IOException {
        try {
            ReadOnlyTransaction transaction = dbClient.readOnlyTransaction(); // Get a read-only transaction
            try {
                Metadata metadata = readFromSpanner(read.getEntity(), transaction).getMetadata();
                return filterMetadata(metadata, KEEP, read.getKinds(),
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
                ? doScroll(request) : doSearch(request);
    }
    private io.cdap.cdap.spi.metadata.SearchResponse doScroll(SearchRequest request) throws IOException {
        Cursor cursor = Cursor.fromString(request.getCursor());
        return performSpannerSearch(request, cursor);
    }

    private io.cdap.cdap.spi.metadata.SearchResponse doSearch(SearchRequest request) throws IOException {
        return performSpannerSearch(request, null);
    }

    private io.cdap.cdap.spi.metadata.SearchResponse performSpannerSearch(SearchRequest request,
                                                                          @Nullable Cursor cursor) throws IOException {
        DatabaseClient dbClient = getDbClient();
        try (ReadOnlyTransaction transaction = dbClient.readOnlyTransaction()) {

            String sql = buildSpannerQuery(request, cursor);
            Statement statement = Statement.newBuilder(sql).build();
            ResultSet resultSet = transaction.executeQuery(statement);

            LOG.info(sql);
            LOG.info(statement.toString());
            LOG.info(resultSet.toString());

            List<MetadataRecord> results = new ArrayList<>();
            String nextCursor = null;

            while (resultSet.next()) {
                results.add(mapSpannerResult(resultSet));
                nextCursor = createNextCursorKey(resultSet);
            }

            if (results.isEmpty()) {
                nextCursor = null;
            }
            LOG.info(results.toString());
            return createSpannerSearchResponse(request, results, nextCursor);
        } catch (SpannerException e) {
            throw new IOException("Spanner search failed", e);
        }
    }
    private MetadataRecord mapSpannerResult(ResultSet resultSet) {
        String documentId = resultSet.getString("metadata_id");
        Struct row = resultSet.getCurrentRowAsStruct();
        LOG.info(row.toString());
        String metadataString = row.getJson(9);
        Metadata metadata = parseMetadataFromJson(metadataString);
        MetadataEntity entity = toMetadataEntity(documentId);


        MetadataRecord record = new MetadataRecord(entity, metadata);
        return record;
    }

    private Metadata parseMetadataFromJson(String metadataJson) {
        if (StringUtils.isEmpty(metadataJson)) {
            return Metadata.EMPTY;
        }

        Type type = new TypeToken<Map<String, Object>>() {}.getType();
        Map<String, Object> metadataMap = gson.fromJson(metadataJson, type);

        Set<ScopedName> tags = new HashSet<>();
        Map<ScopedName, String> properties = new HashMap<>();

        if (metadataMap != null) {
            for (Map.Entry<String, Object> topLevelEntry : metadataMap.entrySet()) {
                String topLevelKey = topLevelEntry.getKey();
                Object topLevelValue = topLevelEntry.getValue();
                LOG.info(topLevelKey, topLevelValue);

                if ("tags".equals(topLevelKey) && topLevelValue instanceof List) {
                    for (Object tagValue : (List<?>) topLevelValue) {
                        if (tagValue instanceof String) {
                            String fullTag = (String) tagValue;
                            String[] parts = fullTag.split(":", 2);
                            MetadataScope scope = MetadataScope.USER;

                            if (parts.length == 2) {
                                try {
                                    scope = MetadataScope.valueOf(parts[0].toUpperCase());
                                    tags.add(new ScopedName(scope, parts[1]));
                                } catch (IllegalArgumentException e) {
                                    LOG.warn("Invalid MetadataScope in tag: " + fullTag + ". Defaulting to USER.");
                                    tags.add(new ScopedName(scope, fullTag));
                                }
                            } else {
                                tags.add(new ScopedName(scope, fullTag));
                            }
                        }
                    }
                } else if ("properties".equals(topLevelKey) && topLevelValue instanceof Map) {
                    // Process the inner map for properties
                    Map<String, Object> propertyMap = (Map<String, Object>) topLevelValue;
                    for (Map.Entry<String, Object> propertyEntry : propertyMap.entrySet()) {
                        String propertyKey = propertyEntry.getKey();
                        Object propertyValue = propertyEntry.getValue();
                        String[] parts = propertyKey.split(":", 2);
                        MetadataScope scope = MetadataScope.USER;
                        String name = propertyKey;

                        if (parts.length == 2) {
                            try {
                                scope = MetadataScope.valueOf(parts[0].toUpperCase());
                                name = parts[1];
                            } catch (IllegalArgumentException e) {
                                LOG.warn("Invalid MetadataScope in property key: " + propertyKey + "." +
                                        " Using full key as name with default USER scope.");
                            }
                        }

                        if (propertyValue instanceof String) {
                            properties.put(new ScopedName(scope, name), (String) propertyValue);
                        } else if (propertyValue instanceof Boolean) {
                            properties.put(new ScopedName(scope, name), propertyValue.toString());
                        } else if (propertyValue instanceof Number) {
                            properties.put(new ScopedName(scope, name), propertyValue.toString());
                        }
                    }
                }
            }
        }

        return new Metadata(tags, properties);
    }

    private String createNextCursorKey(ResultSet resultSet) {
        return resultSet.getString("Namespace") + "," + resultSet.getString("Name")
                + "," + resultSet.getString("Entity_type");
    }

    private io.cdap.cdap.spi.metadata.SearchResponse createSpannerSearchResponse(SearchRequest request,
                                                                                 List<MetadataRecord> results,
                                                                                 String nextCursor) {
        return new io.cdap.cdap.spi.metadata.SearchResponse(request, nextCursor, request.getOffset(),
                request.getLimit(), results.size(), results);
    }

    private static String mapSortKey(String key) {
        // Map SearchRequest sort keys to Spanner column names
        // Example:
        if ("Name".equals(key)) {
            return "Name";
        }
        if ("Namespace".equals(key)) {
            return "Namespace";
        }
        if ("Entity_type".equals(key)) {
            return "Entity_type";
        }
        throw new IllegalArgumentException("Unsupported sort key: " + key);
    }

    private String buildSpannerQuery(SearchRequest request, @Nullable Cursor cursor) {
        StringBuilder sql = new StringBuilder("SELECT * FROM metadata WHERE 1=1 ");

        if (cursor != null && cursor.getActualCursor() != null && !cursor.getActualCursor().isEmpty()) {
            sql.append(" AND (Namespace, Name, Entity_type) > ('").append(cursor.getActualCursor()).append("')");
        }

        if (request.getNamespaces() != null && !request.getNamespaces().isEmpty()) {
            sql.append(" AND Namespace IN ('").append(String.join("','", request.getNamespaces())).append("')");
        }

        if (request.getTypes() != null && !request.getTypes().isEmpty()) {
            sql.append(" AND Entity_type IN ('").append(String.join("','", request.getTypes())).append("')");
        }

        if (request.getQuery() != null && !request.getQuery().isEmpty() && !request.getQuery().equals("*")) {
            List<String> allSearchConditions = new ArrayList<>();
            Iterable<String> terms = Splitter.on(SPACE_SEPARATOR_PATTERN)
                    .omitEmptyStrings().trimResults().split(request.getQuery());

            for (String rawTerm : terms) {
                String cleanedTerm = rawTerm.replace("*", "");

                List<String> termConditions = createSearchConditionsForTerm
                        (request,cleanedTerm.replace("*", ""));
                if (!termConditions.isEmpty()) {
                    allSearchConditions.add("(" + String.join(" OR ", termConditions) + ")");
                }
            }

            if (!allSearchConditions.isEmpty()) {
                sql.append(" AND (").append(String.join(" OR ", allSearchConditions)).append(")");
            }
        } else if (request.getTypes() == null || request.getTypes().isEmpty()) {
            sql.append(" AND Entity_type = 'dataset'");
        }

        if (request.getSorting() != null) {
            sql.append(" ORDER BY ").append(mapSortKey(request.getSorting().getKey().toLowerCase())).append(" ").
                    append(request.getSorting().getOrder().name());
        } else {
            sql.append(" ORDER BY name, Entity_type");
        }

        sql.append(" LIMIT ").append(request.getLimit());

        return sql.toString();
    }

    private List<String> createSearchConditionsForTerm(SearchRequest request,String term) {
        List<String> conditions = new ArrayList<>();

        String query = request.getQuery();

        if (query.contains(KEYVALUE_SEPARATOR)) {
            String[] split = query.split(KEYVALUE_SEPARATOR, 2);
            if (split.length == 2) {
                String field = split[0].trim().toLowerCase();
                String spannerSql = getString(split, field);


                LOG.info("Constructed Spanner Query (KeyValue Pattern): {}", spannerSql);
                conditions.add(spannerSql);
            }
        }
        else {
            if (request.getScope() == MetadataScope.USER) {
                conditions.add(USERScopeSearchQuery(term));
            }
            else if(request.getScope() == MetadataScope.SYSTEM){
                conditions.add(SYSTEMScopeSearchQuery(term));
            }
            else{
                conditions.add(ScopeSearchQuery(term));
            }
        }
            return conditions;
    }


    private String getString(String[] split, String field) {
        String value = split[1].trim().toLowerCase().replace("*", "%");

        return String.format(
                "EXISTS (SELECT 1 FROM UNNEST(JSON_QUERY_ARRAY(%s)) AS element " +
                        "WHERE LOWER(JSON_VALUE(element, '$.name')) = '%s' " +
                        "AND LOWER(JSON_VALUE(element, '$.value')) LIKE '%s')",
                "metadata",
                field,
                value
        );
    }

    private String USERScopeSearchQuery(String value) {
        return "SEARCH_NGRAMS(USER_Substrings, \"" + value + "\")";
    }

    private String SYSTEMScopeSearchQuery(String value) {
        return "SEARCH_NGRAMS(SYSTEM_Substrings, \"" + value + "\")";
    }

    private String ScopeSearchQuery(String value) { /*Union All Logic*/
        return "SEARCH_NGRAMS(USER_Substrings, \"" + value + "\") AND " +
                "SEARCH_NGRAMS(SYSTEM_Substrings, \"" + value + "\")";
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

    @VisibleForTesting
    @SuppressWarnings("ConstantConditions")
    static Metadata filterMetadata(Metadata metadata, boolean keep, Set<MetadataKind> kinds,
                                   Set<MetadataScope> scopes, Set<ScopedNameOfKind> selection) {
        if (selection != null) {
            return new Metadata(
                    Sets.filter(metadata.getTags(), tag ->
                            keep == selection.contains(
                                    new ScopedNameOfKind(MetadataKind.TAG, tag.getScope(), tag.getName()))),
                    Maps.filterKeys(metadata.getProperties(), key ->
                            keep == selection.contains(
                                    new ScopedNameOfKind(MetadataKind.PROPERTY, key.getScope(), key.getName())))
            );
        }
        return new Metadata(
                Sets.filter(metadata.getTags(), tag ->
                        keep == (kinds.contains(MetadataKind.TAG) && scopes.contains(tag.getScope()))),
                Maps.filterKeys(metadata.getProperties(), key ->
                        keep == (kinds.contains(MetadataKind.PROPERTY) && scopes.contains(key.getScope()))));
    }

    private static String toDocumentId(MetadataEntity entity) {
        StringBuilder builder = new StringBuilder(entity.getType());
        char sep = ':';
        for (MetadataEntity.KeyValue kv : entity) {
            // TODO (CDAP-13597): Handle versioning of metadata entities in a better way
            // if it is a versioned entity then ignore the version
            if (MetadataUtil.isVersionedEntityType(entity.getType())
                    && MetadataEntity.VERSION.equalsIgnoreCase(kv.getKey())) {
                continue;
            }
            builder.append(sep).append(kv.getKey()).append('=').append(kv.getValue());
            sep = ',';
        }
        return builder.toString();
    }

    /**
     * Translate a document id in the index into a metadata entity.
     */
    private static MetadataEntity toMetadataEntity(String documentId) {
        int index = documentId.indexOf(':');
        if (index < 0) {
            throw new IllegalArgumentException(
                    "Document Id must be of the form 'type:k=v,...' but is " + documentId);
        }
        String type = documentId.substring(0, index);
        MetadataEntity.Builder builder = MetadataEntity.builder();
        for (String part : documentId.substring(index + 1).split(",")) {
            String[] parts = part.split("=", 2);
            if (parts[0].equals(type)) {
                builder.appendAsType(parts[0], parts[1]);
            } else {
                builder.append(parts[0], parts[1]);
            }
        }
        // TODO (CDAP-13597): Handle versioning of metadata entities in a better way
        // if it is a versioned entity then add the default version
        return MetadataUtil.addVersionIfNeeded(builder.build());
    }

    @Override
    public Object getDatasetMetadata(String datasetName) {
        return Collections.emptyMap();
    }

    public String getProjectId(Map<String, String> conf){
         return conf.get(projectId);
    }
    public String getDatabaseId(Map<String, String> conf){
        return conf.get(databaseId);
    }
    public String getInstanceId(Map<String, String> conf){
        return conf.get(instanceId);
    }

}