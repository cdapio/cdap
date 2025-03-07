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
import com.google.gson.Gson;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.Struct;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.MetadataStorageContext;
import io.cdap.cdap.spi.metadata.MetadataChange;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.ScopedName;
import io.cdap.cdap.spi.metadata.MutationOptions;
import io.cdap.cdap.spi.metadata.Read;
import io.cdap.cdap.spi.metadata.SearchRequest;
import io.cdap.cdap.spi.metadata.ScopedNameOfKind;
import io.cdap.cdap.spi.metadata.MetadataRecord;
import io.cdap.cdap.spi.metadata.MetadataKind;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.spanner.TransactionContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.Set;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.util.concurrent.ExecutionException;

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
    private static final String CREATED_FIELD = "created";
    private static final String HIDDEN_FIELD = "hidden";
    private static final String TYPE_FIELD = "type";
    private static final String TEXT_FIELD = "text";

    private final String instanceId = "cdf-komalyd-instance-test";
    private final String databaseId = "cdap";
    private final String projectId = "da3c84c7b9685e836-tp";
    private volatile SpannerOptions options;
    private volatile Spanner spanner;
    private volatile DatabaseClient dbClient;
    private volatile DatabaseAdminClient adminClient;
    private volatile boolean created;
    private static final String CREDENTIALS_PATH = "credentials.path";

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
                createMetadataTable();
                LOG.info("Metadata table created or verified successfully in Spanner database '{}'", databaseId);
                created = true;
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
                "CREATE TABLE IF NOT EXISTS %s (%s STRING(MAX) NOT NULL, %s STRING(MAX) NOT NULL, %s TIMESTAMP, " +
                        "%s BOOL, %s STRING(MAX), %s STRING(MAX)) PRIMARY KEY (%s, %s)",
                METADATA_TABLE, NAMESPACE_FIELD, NAME_FIELD, CREATED_FIELD, HIDDEN_FIELD, TYPE_FIELD,
                TEXT_FIELD, NAMESPACE_FIELD, NAME_FIELD);
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
            TransactionRunner runner = getDbClient().readWriteTransaction(); // Use getDbClient()
            return runner.run(new TransactionRunner.TransactionCallable<MetadataChange>() {
                @Override
                public MetadataChange run(TransactionContext transaction) throws Exception {
                    Metadata existingMetadata = readFromSpanner(entity);
                    Metadata updatedMetadata = applyMutation(existingMetadata, mutation, transaction);
                    return createMetadataChange(mutation, existingMetadata, updatedMetadata);
                }
            });
        } catch (SpannerException e) {
            throw new IOException("Error applying mutation to Spanner: " + e.getMessage(), e);
        }
    }

    private MetadataChange createMetadataChange(MetadataMutation mutation, Metadata existingMetadata, 
                                                Metadata updatedMetadata) {
        return new MetadataChange(mutation.getEntity(), existingMetadata, updatedMetadata);
    }

    private Metadata readFromSpanner(MetadataEntity entity) throws IOException {
        try (ReadOnlyTransaction transaction = getDbClient().readOnlyTransaction()) {
            String namespace = entity.getValue(MetadataEntity.NAMESPACE);
            String name = entity.getValue(MetadataEntity.APPLICATION);

            if (namespace == null || name == null) {
                LOG.error("Namespace or name is null for entity: {}", entity);
                return Metadata.EMPTY;
            }

            Key key = Key.of(namespace, name);

            ResultSet resultSet = transaction.read(
                    METADATA_TABLE,
                    KeySet.singleKey(key),
                    Arrays.asList(NAMESPACE_FIELD, NAME_FIELD, CREATED_FIELD, HIDDEN_FIELD, TYPE_FIELD, TEXT_FIELD)
            );

            if (resultSet.next()) {
                Map<String, String> properties = new HashMap<>();
                properties.put(NAMESPACE_FIELD, resultSet.getString(NAMESPACE_FIELD));
                properties.put(NAME_FIELD, resultSet.getString(NAME_FIELD));
                if (!resultSet.isNull(CREATED_FIELD)) {
                    properties.put(CREATED_FIELD, String.valueOf(resultSet.getTimestamp(CREATED_FIELD)
                            .toSqlTimestamp().getTime()));
                }
                if (!resultSet.isNull(HIDDEN_FIELD)) {
                    properties.put(HIDDEN_FIELD, String.valueOf(resultSet.getBoolean(HIDDEN_FIELD)));
                }
                if (!resultSet.isNull(TYPE_FIELD)) {
                    properties.put(TYPE_FIELD, resultSet.getString(TYPE_FIELD));
                }
                if (!resultSet.isNull(TEXT_FIELD)) {
                    properties.put(TEXT_FIELD, resultSet.getString(TEXT_FIELD));
                }

                Map<ScopedName, String> scopedProperties = new HashMap<>();
                properties.forEach((propKey, propValue) -> scopedProperties
                        .put(new ScopedName(MetadataScope.USER, propKey), propValue));

                // Create Metadata with empty tags and scoped properties.
                Metadata metadata = new Metadata(Collections.emptySet(), scopedProperties);
                return metadata;
            } else {
                return Metadata.EMPTY;
            }
        } catch (SpannerException e) {
            throw new IOException("Failed to read from Spanner for entity " + entity, e);
        }
    }

    private Metadata applyMutation(Metadata before, MetadataMutation mutation, TransactionContext transaction) {
        LOG.trace("Applying mutation {} to entity {} with metadata {}",
                mutation, mutation.getEntity(), before);
        switch (mutation.getType()) {
            case CREATE:
                return create(before, (MetadataMutation.Create) mutation, transaction);
            case DROP:
                return drop(before, mutation.getEntity(), transaction);
            case UPDATE:
                return update(before, (MetadataMutation.Update) mutation, transaction);
            case REMOVE:
                return remove(before, (MetadataMutation.Remove) mutation, transaction);
            default:
                throw new IllegalStateException(String.format("Unknown mutation type '%s' for %s",
                        mutation.getType(), mutation));
        }
    }

    private Metadata create(Metadata before, MetadataMutation.Create create, TransactionContext transaction) {
        // Check if the entity already exists in Spanner
        if (!before.isEmpty()) {
            //If it exists, throw an exception.
            throw new IllegalStateException("Entity already exists: " + create.getEntity());
        }

        Metadata meta = create.getMetadata();

        // In Spanner, we directly write the new metadata.
        writeToSpanner(transaction, create.getEntity(), meta); // Use transaction
        return meta;
    }

    private Metadata drop(Metadata before, MetadataEntity entity, TransactionContext transaction) {
        // In Spanner, we directly delete the entity.
        deleteFromSpanner(transaction, entity); // Use transaction

        // Return Metadata.EMPTY, indicating the entity is dropped.
        return Metadata.EMPTY;
    }

    private void deleteFromSpanner(TransactionContext transaction, MetadataEntity entity) {
        try {
            Key key = Key.of(entity.getValue(MetadataEntity.NAMESPACE), entity.getValue(MetadataEntity.APPLICATION));
            transaction.buffer(Mutation.delete(METADATA_TABLE, KeySet.singleKey(key)));
        } catch (SpannerException e) {
            LOG.error("Error deleting from spanner", e);
            throw e;
        }
    }

    private Metadata update(Metadata before, MetadataMutation.Update update, TransactionContext transaction) {
        MetadataEntity entity = update.getEntity();
        Metadata updates = update.getUpdates();

        // Merge tags and properties (if needed)
        Map<String, String> mergedProperties = new HashMap<>();
        if (!before.isEmpty()) {
            mergedProperties.putAll(before.getProperties(MetadataScope.USER)); // Assumes USER scope
        }
        mergedProperties.putAll(updates.getProperties(MetadataScope.USER)); // Assumes USER scope

        // Create new Metadata object
        Metadata after = new Metadata(MetadataScope.USER, mergedProperties);

        // Write to Spanner
        writeToSpanner(transaction, entity, after); // Use transaction

        return after;
    }

    private Metadata remove(Metadata before, MetadataMutation.Remove remove, TransactionContext transaction) {
        Metadata after = filterMetadata(before, remove.getKinds(), remove.getScopes(), remove.getRemovals());
        writeToSpanner(transaction, remove.getEntity(), after); // Use transaction
        return after;
    }

    private void writeToSpanner(TransactionContext transaction, MetadataEntity entity, Metadata metadata) {
        try {
            Mutation.WriteBuilder writeBuilder = Mutation.newInsertOrUpdateBuilder(METADATA_TABLE);

            writeBuilder.set(NAMESPACE_FIELD).to(entity.getValue(MetadataEntity.NAMESPACE));
            writeBuilder.set(NAME_FIELD).to(entity.getValue(MetadataEntity.APPLICATION));

            // Convert the properties to a JSON string.
            Map<ScopedName, String> properties = metadata.getProperties();
            Gson gson = new Gson();
            String propertiesJson = gson.toJson(properties.entrySet().stream()
                    .collect(java.util.stream.Collectors.toMap(e -> e.getKey().getName(),
                            Map.Entry::getValue)));

            writeBuilder.set("properties").to(Value.string(propertiesJson)); // Store JSON as a string

            transaction.buffer(writeBuilder.build()); // Use transaction buffer

        } catch (SpannerException e) {
            LOG.error("Error writing to Spanner", e);
            throw e;
        }
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

        List<MetadataChange> changes = new ArrayList<>(mutations.size());
        try {
            TransactionRunner runner = getDbClient().readWriteTransaction(); // Use getDbClient()
            runner.run(new TransactionRunner.TransactionCallable<Void>() {
                @Override
                public Void run(TransactionContext transaction) throws Exception {
                    for (MetadataMutation mutation : mutations) {
                        MetadataEntity entity = mutation.getEntity();
                        Metadata existingMetadata = readFromSpanner(entity);
                        Metadata updatedMetadata = applyMutation(existingMetadata, mutation, transaction);
                        writeToSpanner(transaction, entity, updatedMetadata);
                        changes.add(createMetadataChange(mutation, existingMetadata, updatedMetadata));
                    }
                    return null;
                }
            });
            return changes;
        } catch (SpannerException e) {
            throw new IOException("Error applying batch mutations to Spanner: " + e.getMessage(), e);
        }
    }

    @Override
    public Metadata read(Read read) throws IOException {
        try {
            Metadata metadata = readFromSpanner(read.getEntity());
            return filterMetadata(metadata, read.getKinds(), read.getScopes(), read.getSelection());
        } catch (SpannerException e) {
            throw new IOException("Failed to read metadata from Spanner: " + e.getMessage(), e);
        }
    }


    @Override
    public io.cdap.cdap.spi.metadata.SearchResponse search(SearchRequest request) throws IOException {
        return doSearch(request);
    }

    private io.cdap.cdap.spi.metadata.SearchResponse doSearch(SearchRequest request) throws IOException {
        try {
            Statement statement = buildSpannerQuery(request);
            List<Metadata> results = executeSpannerQuery(statement);

            // Retrieve the MetadataEntity from the context.
            MetadataEntity entity = getMetadataEntityFromContext(request); // Replace with your logic

            return createSearchResponse(request, results, entity); // Pass the entity
        } catch (SpannerException e) {
            throw new IOException("Failed to search metadata in Spanner: " + e.getMessage(), e);
        }
    }

    private MetadataEntity getMetadataEntityFromContext(SearchRequest request) {
        MetadataEntity.Builder builder = MetadataEntity.builder();

        // Assumption: Use the first namespace from the request (if available).
        if (request.getNamespaces() != null && !request.getNamespaces().isEmpty()) {
            String namespace = request.getNamespaces().iterator().next();
            builder.append(MetadataEntity.NAMESPACE, namespace);
        } else {
            // Assumption: Use a default namespace if none is provided.
            builder.append(MetadataEntity.NAMESPACE, "default"); // Replace with your default namespace
        }

        // We cannot reliably determine the application from the SearchRequest itself.
        // We will need to set it from the results.

        return builder.build();
    }

    private Statement buildSpannerQuery(SearchRequest request) {
        // Implement Spanner query building logic here, similar to createSearchSource and createQuery.
        // Use request.getOffset(), request.getLimit(), request.getSorting(), etc.
        StringBuilder queryBuilder = new StringBuilder("SELECT * FROM " + METADATA_TABLE + " WHERE 1=1");
        //Add where clauses based on the searchRequest.
        //Add limit and offset.
        int limit = request.getLimit();
        int offset = request.getOffset();
        queryBuilder.append(" LIMIT ").append(limit).append(" OFFSET ").append(offset);
        return Statement.of(queryBuilder.toString());
    }

    private List<Metadata> executeSpannerQuery(Statement statement) {
        // Execute Spanner query and convert results to Metadata objects.
        List<Metadata> results = new ArrayList<>();
        try (ResultSet resultSet = getDbClient().singleUse().executeQuery(statement)) {
            while (resultSet.next()) {
                results.add(rowToMetadata(resultSet.getCurrentRowAsStruct()));
            }
        }
        return results;
    }

    private Metadata rowToMetadata(Struct row) {
        Map<String, String> properties = new Gson().fromJson(row.getString("properties"), Map.class);
        Map<ScopedName, String> scopedProperties = new HashMap<>();
        properties.forEach((propKey, propValue) ->
                scopedProperties.put(new ScopedName(MetadataScope.USER, propKey), propValue));

        // Extract application/entity identifier (replace "application" with your column name).
        String application = row.getString("application");

        // Store the identifier as a property.
        if (application != null) {
            scopedProperties.put(new ScopedName(MetadataScope.USER, "application"), application);
        }

        return new Metadata(Collections.emptySet(), scopedProperties);
    }

    private io.cdap.cdap.spi.metadata.SearchResponse createSearchResponse(SearchRequest request, List<Metadata> results,
                                                                          MetadataEntity baseEntity) {
        List<MetadataRecord> metadataRecords = new ArrayList<>();
        for (Metadata metadata : results) {
            // Retrieve the application/entity identifier.
            String application = metadata.getProperties().get(new ScopedName(MetadataScope.USER, "application"));

            // Create a new MetadataEntity with the identifier.
            MetadataEntity entity;
            if (application != null) {
                entity = MetadataEntity.builder(baseEntity)
                        .append(MetadataEntity.APPLICATION, application) // Or your entity type
                        .build();
            } else {
                entity = baseEntity; // If no application is found, use the base entity.
            }

            metadataRecords.add(new MetadataRecord(entity, metadata));
        }

        return new io.cdap.cdap.spi.metadata.SearchResponse(request, null, request.getOffset(),
                request.getLimit(), metadataRecords.size(), metadataRecords);
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
        Map<String, String> filteredProperties = new HashMap<>();
        Map<String, String> properties = metadata.getProperties(MetadataScope.USER); // Assumes USER scope

        if (selection != null) {
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                ScopedNameOfKind scopedNameOfKind = new ScopedNameOfKind(MetadataKind.PROPERTY, MetadataScope.USER,
                        entry.getKey());
                if (!selection.contains(scopedNameOfKind)) {
                    filteredProperties.put(entry.getKey(), entry.getValue());
                }
            }
        } else {
            if (!kinds.contains(MetadataKind.PROPERTY) || !scopes.contains(MetadataScope.USER)) {
                filteredProperties.putAll(properties);
            }
        }

        return new Metadata(MetadataScope.USER, filteredProperties);
    }

    @Override
    public Object getDatasetMetadata(String datasetName) {
        return Collections.emptyMap();
    }

}