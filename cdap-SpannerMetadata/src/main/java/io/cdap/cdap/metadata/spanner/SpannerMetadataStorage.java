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
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.SpannerException;
import com.google.common.collect.ImmutableMap;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.MetadataStorageContext;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.MetadataChange;
import io.cdap.cdap.spi.metadata.MutationOptions;
import io.cdap.cdap.spi.metadata.Read;
import io.cdap.cdap.spi.metadata.SearchRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * SpannerMetadataStorage implements the MetadataStorage interface
 * using Google Cloud Spanner as the underlying storage.
 * It provides methods for managing metadata within a Spanner database.
 */
public class SpannerMetadataStorage implements MetadataStorage {

    private static final Logger LOG = LoggerFactory.getLogger(SpannerMetadataStorage.class);
    // Various fields in a metadata document (indexed in elasticsearch). Beware that these directly
    // correspond to field name in the index settings (index.mapping.json). Any change here must be
    // reflected there, and vice versa.
    private static final String CREATED_FIELD = "created";
    private static final String HIDDEN_FIELD = "hidden";
    private static final String NAME_FIELD = "name";
    private static final String NAMESPACE_FIELD = "namespace";
    private static final String PROPS_FIELD = "props";
    private static final String TEXT_FIELD = "text";
    private static final String TYPE_FIELD = "type";
    // these are the only fields that are supported for sorting
    private static final Map<String, String> SORT_KEY_MAP = ImmutableMap.of(
            "entity-name", NAME_FIELD,
            "creation-time", CREATED_FIELD
    );
    private static final String SUPPORTED_SORT_KEYS = String.join(", ", SORT_KEY_MAP.keySet());
    // Spanner-specific settings
    private final String instanceId = "cdf-komalyd-instance-test";
    private final String databaseId = "cdap";
    private final String projectId = "da3c84c7b9685e836-tp";
    private final int numRetries = 50;
    private final int retrySleepMs = 100;
    private volatile SpannerOptions options;
    private volatile Spanner spanner;
    private volatile DatabaseClient dbClient;
    private volatile boolean created;


    // sleep 100 ms for at most 50 times
    private RetryStrategy retryStrategyOnConflict;

    @Override
    public void initialize(MetadataStorageContext context) throws Exception {
        try {
            options = SpannerOptions.newBuilder().setProjectId(projectId).build();
            spanner = options.getService();
            dbClient = spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));
            LOG.info("Successfully initialized Spanner client for instance: {}, database: {}",
                    instanceId, databaseId);
        } catch (SpannerException e) {
            LOG.error("Error initializing Spanner client: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to initialize Spanner client", e);
        }
    }

    @Override
    public void close() {
        if (spanner != null) {
            spanner.close();
        }
    }

    @Override
    public void createIndex() throws IOException {
        if (created) {
            return;
        }
        synchronized (this) {
            if (created) {
                return;
            }


            DatabaseAdminClient dbAdminClient = null; // Declare outside the try block

            try {
                DatabaseClient spannerClient = getDbClient(); // Explicit test call
                LOG.info("Successfully retrieved (or created) Spanner client in createIndex: {}",
                        spannerClient);
                dbAdminClient = spanner.getDatabaseAdminClient();

                DatabaseId dbId = DatabaseId.of(projectId, instanceId, databaseId);


                try {
                    dbAdminClient.getDatabase(instanceId, databaseId);
                    LOG.info("Spanner database '{}' already exists.", databaseId);
                    created = true;
                    return;
                } catch (SpannerException e) {
                    if (e.getErrorCode() != ErrorCode.NOT_FOUND) {
                        LOG.error("Error checking for database existence: {}", e.getMessage(), e);
                        throw new IOException("Error checking database: " + e.getMessage(), e);
                    }
                } catch (RuntimeException e) {
                    LOG.error("Error in Spanner Client creation: {}", e.getMessage(), e);
                }

                LOG.info("Creating Spanner database '{}'", databaseId);
                List<String> statements = new ArrayList<>();
                statements.add("CREATE TABLE Metadata (" +
                        "  namespace STRING(MAX)," +
                        "  name STRING(MAX)," +
                        "  created TIMESTAMP," +
                        "  hidden BOOL," +
                        "  type STRING(MAX)," +
                        "  text STRING(MAX)," +
                        "  PRIMARY KEY (namespace, name)" +
                        ")");

                OperationFuture<Database, CreateDatabaseMetadata> operation =
                        dbAdminClient.createDatabase(instanceId, databaseId, statements);

                try {
                    operation.get(); // Wait for the operation to complete
                    created = true;
                } catch (InterruptedException | ExecutionException e) {
                    LOG.error("Database creation failed: {}", e.getMessage(), e);
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt(); // Preserve interrupt status
                    }
                    throw new IOException("Database creation failed: " + e.getMessage(), e);
                }

            } catch (SpannerException e) {
                LOG.error("Error creating Spanner database", e);
                throw new IOException("Error creating Spanner: " + e.getMessage(), e);
            }

            created = true;
        }
    }

    @Override
    public String getName() {
        return "spanner";
    }

    @Override
    public void dropIndex() throws IOException {

    }


    @Override
    public MetadataChange apply(MetadataMutation mutation, MutationOptions options)
            throws IOException {
        return null;
    }

    @Override
    public List<MetadataChange> batch(List<? extends MetadataMutation> mutations,
                                      MutationOptions options) throws IOException {
        return new ArrayList<>();
    }

    @Override
    public Metadata read(Read read) throws IOException {
        return null;
    }

    @Override
    public io.cdap.cdap.spi.metadata.SearchResponse search(SearchRequest request)
            throws IOException {
        return null;
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

    @Override
    public Object getDatasetMetadata(String datasetName) {
        return Collections.emptyMap();
    }

}