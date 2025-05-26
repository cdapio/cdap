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
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.metadata.Cursor;
import io.cdap.cdap.common.metadata.MetadataConflictException;
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

import javax.annotation.Nullable;
import java.util.Set;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Objects;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Arrays;


import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;



import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

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
    private static final String TEXT_FIELD = "Text"; // storing all searchable values
    private static final String NESTED_NAME_FIELD = "Props_Name"; // contains the property name in nested props
    private static final String NESTED_SCOPE_FIELD = "Props_Scope"; // contains the scope in nested props
    private static final String NESTED_VALUE_FIELD = "Props_Value"; // contains the value in nested props

    private  String instanceId = "instance";
    private  String databaseId = "database";
    private  String projectId = "project";
    private volatile SpannerOptions options;
    private volatile Spanner spanner;
    private volatile DatabaseClient dbClient;
    private volatile DatabaseAdminClient adminClient;
    private volatile boolean created;

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
        projectId=getProjectId(conf);
        databaseId=getDatabaseId(conf);
        instanceId=getInstanceId(conf);
        LOG.info(projectId,databaseId,instanceId);
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

    }

    @Override
    public void createIndex() throws IOException {

    }

    private boolean tableExists() {
       return false;
    }

    private void createMetadataTable() throws IOException, InterruptedException, ExecutionException {
        LOG.info("Creating Metadata table...");
        List<String> ddlStatements = new ArrayList<>();
        ddlStatements.add(MetadataTableDDLStatement());
        ddlStatements.add(Metadata_propsTableDDLStatement());
        ddlStatements.add(getUSERSearchIndexDDLStatement());
        ddlStatements.add(getSYSTEMSearchIndexDDLStatement());
        ddlStatements.add(getValueSearchIndexDDLStatement());
        executeCreateDDLStatements(ddlStatements);
        LOG.info("Metadata table creation completed.");
    }

    private String MetadataTableDDLStatement() {
        LOG.info("Generating create table DDL statement.");
        return String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        "metadata_id STRING(MAX) NOT NULL,"+
                        "%s STRING(MAX) NOT NULL," + // namespace
                        "%s STRING(MAX) NOT NULL," + // entity_type
                        "%s STRING(MAX) NOT NULL," + // name
                        "%s INT64," + // created
                        "%s INT64," + // ttl
                        "%s BOOL," + // hidden
                        "%s STRING(MAX)," + // user
                        "%s STRING(MAX)," + // system
                        "%s STRING(MAX)," + // text
                        "properties JSON," + // properties as JSON
                        "metadata_column JSON," + // metadata
                        "VERSION INT64 NOT NULL," +
                        "USER_Substrings TOKENLIST AS " +
                        "(TOKENIZE_SUBSTRING(USER, ngram_size_min=>1, ngram_size_max=>3)) HIDDEN,"+
                        "SYSTEM_Substrings TOKENLIST AS " +
                        "(TOKENIZE_SUBSTRING(SYSTEM, ngram_size_min=>1, ngram_size_max=>3)) HIDDEN,"+
                        ")PRIMARY KEY (metadata_id) ", // primary key
                METADATA_TABLE,
                NAMESPACE_FIELD,
                TYPE_FIELD,
                NAME_FIELD,
                CREATED_FIELD,
                TTL_FIELD,
                HIDDEN_FIELD,
                User_FIELD,
                SYSTEM_FIELD,
                TEXT_FIELD
        );
    }

    private String Metadata_propsTableDDLStatement() {
        LOG.info("Generating Metadata_props table DDL statement.");
        return String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        "metadata_id STRING(MAX) NOT NULL,"+
                        "%s STRING(MAX) NOT NULL," + // name
                        "%s STRING(MAX)," + // scope
                        "%s STRING(MAX)," + // value
                        "Value_Substrings TOKENLIST AS " +
                        "(TOKENIZE_SUBSTRING(Props_Value, ngram_size_min=>1, ngram_size_max=>3)) HIDDEN,"+
                        ")PRIMARY KEY (metadata_id, %s, %s) ,"+ // primary key
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
        return String.format("CREATE SEARCH INDEX USERNgramIndex ON %s(USER_Substrings)", METADATA_TABLE);
    }

    private String getSYSTEMSearchIndexDDLStatement() {
        return String.format("CREATE SEARCH INDEX SYSTEMNgramIndex ON %s(SYSTEM_Substrings)", METADATA_TABLE);
    }

    private String getValueSearchIndexDDLStatement() {
        return String.format("CREATE SEARCH INDEX ValueNgramIndex ON %s(Value_Substrings)", METADATA_PROPS_TABLE);
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

    public VersionedMetadata readFromSpanner(MetadataEntity entity, TransactionContext transaction) throws IOException
    {return VersionedMetadata.NONE;}

    public VersionedMetadata readFromSpanner(MetadataEntity entity, ReadOnlyTransaction transaction) throws IOException
    {return VersionedMetadata.NONE;}

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
        if (!before.existing()) {
            return update(create.getEntity(), before, create.getMetadata());
        }
        Mutation dummyMutations = writeToSpanner(create.getEntity(), before.getVersion(), create.getMetadata());
        MetadataChange dummyChange = new MetadataChange(create.getEntity(), before.getMetadata(), create.getMetadata());
        return new RequestandChange(dummyMutations, dummyChange);
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
    private Mutation deleteFromSpanner(MetadataEntity entity, Long existingVersion) {
        return Mutation.delete(
                "Metadata",
                Key.of(
                        entity.getValue(MetadataEntity.NAMESPACE),
                        entity.getType(),
                        entity.getValue(entity.getType())
                )
        );
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

    public Mutation writeToSpanner(MetadataEntity entity, Long expectVersion, Metadata metadata) throws IOException {
        Mutation.WriteBuilder mutationBuilder = Mutation.newInsertOrUpdateBuilder("Metadata");
        return mutationBuilder.build();
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

        throw new MetadataConflictException("Spanner batch failed after " + maxRetries + " retries",
                new ArrayList<>(mutations.keySet()));
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

    /**
     * Entry point for performing a metadata search.
     * This method delegates to {@code doScroll} if a cursor is provided, otherwise to {@code doSearch}.
     * Cursors are used for pagination in Spanner, allowing efficient retrieval of large result sets.
     *
     * @param request The {@link SearchRequest} containing search criteria, pagination, and sorting information.
     * @return A {@link SearchResponse} containing the search results and a cursor for the next page, if applicable.
     * @throws IOException If there is an error during the search operation.
     */
    @Override
    public SearchResponse search(SearchRequest request) throws IOException {
        return request.getCursor() != null && !request.getCursor().isEmpty()
                ? doScroll(request) : doSearch(request);
    }

    /**
     * Performs a metadata search using a provided cursor for scrolling/pagination.
     * This method parses the cursor string and then calls {@code performSpannerSearch}.
     *
     * @param request The {@link SearchRequest} containing search criteria.
     * @return A {@link SearchResponse} with results from the current cursor position.
     * @throws IOException If there is an error during the search operation.
     */
    private io.cdap.cdap.spi.metadata.SearchResponse doScroll(SearchRequest request) throws IOException {
        Cursor cursor = Cursor.fromString(request.getCursor());
        return performSpannerSearch(request, cursor);
    }

    /**
     * Perform a search that does continue a previous search using a cursor.
     *
     * @param request the search request
     */
    private io.cdap.cdap.spi.metadata.SearchResponse doSearch(SearchRequest request) throws IOException {
        return performSpannerSearch(request, null);
    }

    /**
     * Executes the actual Spanner query to perform a metadata search.
     * It builds the SQL query based on the search request, executes it in a read-only transaction,
     * maps the results to {@link MetadataRecord} objects, and handles cursor creation for pagination.
     *
     * @param request The {@link SearchRequest} defining the search parameters.
     * @param cursor  An optional {@link Cursor} for pagination; null for the first page.
     * @return A {@link SearchResponse} containing the list of {@link MetadataRecord}s and a cursor for the next page.
     * @throws IOException If an error occurs while interacting with Spanner.
     */
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
        }
    }
    /**
     * Maps a single row from a Spanner {@link ResultSet} to a {@link MetadataRecord} object.
     * This involves extracting column values like namespace, name, entity type, and the JSON-serialized metadata,
     * then parsing the metadata JSON and constructing the appropriate {@link MetadataEntity}.
     *
     * @param resultSet The Spanner {@link ResultSet} positioned at the current row.
     * @return A {@link MetadataRecord} representing the current row's metadata.
     */
    private MetadataRecord mapSpannerResult(ResultSet resultSet) {
        String namespace = resultSet.getString("namespace");
        String name = resultSet.getString("name");
        String entityType = resultSet.getString("entity_type");
        Struct row = resultSet.getCurrentRowAsStruct();
        LOG.info(row.toString());
        String metadataString = row.getJson(9);
        Metadata metadata = parseMetadataFromJson(metadataString);
        MetadataEntity entity;

        if ("plugin".equals(entityType)) {
            // Extract artifact and version from the metadata properties
            String artifact = metadata.getProperties(MetadataScope.SYSTEM).get("artifact");
            String version = metadata.getProperties(MetadataScope.SYSTEM).get("version");
            String pluginName = name;

            if (artifact != null && version != null) {
                entity = MetadataEntity.builder()
                        .append(MetadataEntity.NAMESPACE, namespace)
                        .append("artifact", artifact)
                        .append("version", version)
                        .append("type", "plugin")
                        .append("plugin", pluginName)
                        .build();
            } else {
                LOG.error("Could not find artifact or version for plugin: {}", name);
                return null;
            }
        } else {
            entity = MetadataEntity.builder()
                    .append(MetadataEntity.NAMESPACE, namespace)
                    .append(entityType, name)
                    .build();
        }

        MetadataRecord record = new MetadataRecord(entity, metadata);
        return record;
    }
    /**
     * Parses a JSON string representing metadata into a {@link Metadata} object.
     * This method expects the JSON to contain tags and properties, potentially in various formats,
     * and maps them to {@link ScopedName}s and their corresponding values.
     * Currently, it appears to assume all properties are USER scope and tags are String arrays.
     * This method might need refinement if SYSTEM scope or different tag/property representations exist in
     * `metadataJson`.
     * @param metadataJson The JSON string containing metadata.
     * @return A {@link Metadata} object, or {@link Metadata#EMPTY} if the input is null or empty.
     */

    private Metadata parseMetadataFromJson(String metadataJson) {
        if (metadataJson == null || metadataJson.isEmpty()) {
            return Metadata.EMPTY;
        }

        java.lang.reflect.Type type = new TypeToken<Map<String, Object>>() {}.getType();
        Map<String, Object> metadataMap = gson.fromJson(metadataJson, type);

        Set<ScopedName> tags = new HashSet<>();
        Map<ScopedName, String> properties = new HashMap<>();

        if (metadataMap != null) {
            for (Map.Entry<String, Object> entry : metadataMap.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                if (value instanceof String) {
                    properties.put(new ScopedName(MetadataScope.USER, key), (String) value);
                } else if (value instanceof Boolean) {
                    properties.put(new ScopedName(MetadataScope.USER, key), value.toString());
                } else if (value instanceof Number) {
                    properties.put(new ScopedName(MetadataScope.USER, key), value.toString());
                } else if (value instanceof String[]) {
                    for (String tag : (String[])value){
                        tags.add(new ScopedName(MetadataScope.USER, tag));
                    }
                }
            }
        }

        return new Metadata(tags, properties);
    }

    /**
     * Creates a cursor key string from the current row of a Spanner {@link ResultSet}.
     * This key is used for pagination, allowing subsequent search requests to start from the next record.
     * The cursor is typically formed by the primary key columns of the table, ordered as per the query's ORDER BY clause.
     *
     * @param resultSet The Spanner {@link ResultSet} positioned at the current row.
     * @return A comma-separated string representing the cursor key (namespace, name, entity_type).
     */
    private String createNextCursorKey(ResultSet resultSet) {
        return resultSet.getString("namespace") + "," + resultSet.getString("name")
                + "," + resultSet.getString("entity_type");
    }

    private io.cdap.cdap.spi.metadata.SearchResponse createSpannerSearchResponse(SearchRequest request,
                                                                                 List<MetadataRecord> results,
                                                                                 String nextCursor) {
        return new io.cdap.cdap.spi.metadata.SearchResponse(request, nextCursor, request.getOffset(),
                request.getLimit(), results.size(), results);
    }

    /**
     * Maps a generic sort key from a {@link SearchRequest} to a Spanner column name.
     * This method ensures that the sorting criteria provided in the search request
     * are correctly translated into valid column names for the Spanner SQL query.
     *
     * @param key The sort key string (e.g., "name", "namespace", "entity_type").
     * @return The corresponding Spanner column name.
     * @throws IllegalArgumentException If an unsupported sort key is provided.
     */
    private static String mapSortKey(String key) {
        // Map SearchRequest sort keys to Spanner column names
        // Example:
        if ("name".equals(key)) {
            return "name";
        }
        if ("namespace".equals(key)) {
            return "namespace";
        }
        if ("entity_type".equals(key)) {
            return "entity_type";
        }
        throw new IllegalArgumentException("Unsupported sort key: " + key);
    }

    /**
     * Builds the Spanner SQL query string for metadata search based on the provided {@link SearchRequest} and cursor.
     * This method constructs a SQL query that includes filtering by namespace, entity types,
     * a full-text search condition (on the 'Text' column, or 'metadata_column' potentially),
     * and ordering/limiting for pagination.
     *
     * @param request The {@link SearchRequest} containing search criteria.
     * @param cursor  An optional {@link Cursor} for pagination, used in the WHERE clause for scrolling.
     * @return The constructed SQL query string.
     */
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
            sql.append(" ORDER BY name, Entity_type, Text");
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
                "properties",
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