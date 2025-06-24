/*
 * Copyright © 2025 Cask Data, Inc.
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

import static io.cdap.cdap.metadata.spanner.SpannerMetadataStorage.toMetadataId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.DatabaseNotFoundException;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.InstanceNotFoundException;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataStorageContext;
import io.cdap.cdap.spi.metadata.ScopedName;
import io.cdap.cdap.spi.metadata.VersionedMetadata;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for the Cloud Spanner implementation of the
 * {@link io.cdap.cdap.metadata.spanner.SpannerMetadataStorage}.
 *
 * <h2>Setting Up the Spanner Emulator Locally</h2>
 * To run these tests, you need to set up the Cloud Spanner Emulator on your local machine. Follow
 * these steps:
 *
 * <pre>
 *   gcloud config configurations create emulator
 *   gcloud config set auth/disable_credentials true
 *   gcloud config set api_endpoint_overrides/spanner http://localhost:9010/
 *   gcloud config configurations activate emulator
 *   gcloud emulators spanner start
 * </pre>
 *
 * <h2>Configuring IDE while running the test</h2>
 * To run test, make sure you set the env variable : `SPANNER_EMULATOR_HOST=localhost:9010`. In
 * Intellij, you will need to edit test configuration and add this env variable.
 *
 * <h2>Cleaning Up the Environment</h2>
 * After running the tests, clean up your environment by executing:
 * <pre>
 *   gcloud config unset auth/disable_credentials
 *   gcloud config unset api_endpoint_overrides/spanner
 * </pre>
 * <p>
 * Note: The Cloud Spanner Emulator is intended for local development and testing purposes only. It
 * does not persist data across sessions and is not suitable for production use.
 * <p>
 * NOTE: This test class does not currently extend a base test class {@link MetadataStorageTest} because the
 * {@link SpannerMetadataStorage} implementation is not yet feature-complete. Once all
 * required methods are implemented, this class will be updated to ensure full test coverage.
 */
public class SpannerMetadataStorageTest {

  private static final String PROJECT_ID = "test-project";
  private static final String INSTANCE_ID = "test-instance";
  private static final String DATABASE_ID = "test-database";

  private static SpannerMetadataStorage spannerMetadataStorage;
  private static DatabaseAdminClient dbAdminClient;
  private static DatabaseClient dbClient;
  private static InstanceAdminClient adminClient;
  private static Spanner spanner;

  // Metadata table names
  private static final String METADATA_TABLE = "metadata";
  private static final String METADATA_PROPS_TABLE = "metadata_props";

  // Indicates whether the Spanner emulator is active, guiding cleanup decisions.
  private static boolean isEmulatorRunning;

  @BeforeClass
  public static void setUp() throws Exception {
    // Spanner Emulator host not set. Skipping test.
    String emulatorHost = System.getenv("SPANNER_EMULATOR_HOST");
    Assume.assumeNotNull(emulatorHost, "SPANNER_EMULATOR_HOST should be set");

    SpannerOptions options = SpannerOptions.newBuilder()
      .setEmulatorHost(emulatorHost)
      .setProjectId(PROJECT_ID)
      .build();
    // If emulator is not running, but the env variable SPANNER_EMULATOR_HOST is still set,
    // Exception is thrown here. Thus, isEmulatorRunning variable is not set to true.
    spanner = options.getService();
    dbAdminClient = spanner.getDatabaseAdminClient();
    adminClient = spanner.getInstanceAdminClient();

    // Cleanup of Spanner resources is necessary only if the emulator is operational.
    // Without this check, the test may hang indefinitely due to the asynchronous nature of dropDatabase().
    isEmulatorRunning = true;

    try {
      adminClient.createInstance(InstanceInfo.newBuilder(InstanceId.of(PROJECT_ID, INSTANCE_ID))
                                   .setInstanceConfigId(InstanceConfigId.of(PROJECT_ID,
                                                                            "regional-us-central1"))
                                   .setDisplayName("Test Instance")
                                   .build()).get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      // Instance already exists, proceed.
      if (cause instanceof SpannerException && ((SpannerException) cause).getErrorCode() != ErrorCode.ALREADY_EXISTS) {
        throw e;
      }
    }

    try {
      dbAdminClient.createDatabase(INSTANCE_ID, DATABASE_ID, Collections.emptyList()).get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      // DB already exists, proceed.
      if (cause instanceof SpannerException && ((SpannerException) cause).getErrorCode() != ErrorCode.ALREADY_EXISTS) {
        throw e;
      }
    }

    Map<String, String> configs = new HashMap<>();
    configs.put("project", PROJECT_ID);
    configs.put("instance", INSTANCE_ID);
    configs.put("database", DATABASE_ID);
    configs.put("SPANNER_EMULATOR_HOST", emulatorHost);
    MetadataStorageContext context = new MockMetadataStorageContext(configs);

    spannerMetadataStorage = new SpannerMetadataStorage();
    spannerMetadataStorage.initialize(context);

    // A DatabaseClient represents a session with an existing database, so it must be acquired after creation.
    dbClient = spanner.getDatabaseClient(DatabaseId.of(PROJECT_ID, INSTANCE_ID, DATABASE_ID));
  }

  @AfterClass
  public static void cleanUp() {
    if (isEmulatorRunning) {
      if (dbAdminClient != null) {
        try {
          dbAdminClient.dropDatabase(INSTANCE_ID, DATABASE_ID);
        } catch (DatabaseNotFoundException e) {
        } catch (Exception e) {
          throw new RuntimeException("Failed to drop database during @AfterClass cleanup", e);
        }
      }

      if (adminClient != null) {
        try {
          adminClient.deleteInstance(INSTANCE_ID);
        } catch (InstanceNotFoundException e) {
        } catch (Exception e) {
          throw new RuntimeException("Failed to delete instance during @AfterClass cleanup", e);
        }
      }

      if (spanner != null) {
        spanner.close();
        spanner = null;
      }
    }
  }

  @Before
  public void beforeTest() throws IOException {
    spannerMetadataStorage.createIndex();
  }

  /**
   * Helper method to simulate initial metadata creation by directly inserting into Spanner.
   * This is used because `create` and `writeToSpanner` in `SpannerMetadataStorage` are "NOT IMPLEMENTED".
   * In a real scenario, these would be part of the `MetadataStorage` implementation.
   */
  private void simulateInitialMetadata(MetadataEntity entity, Metadata metadata, long version) throws IOException {
    try {
      dbClient.readWriteTransaction().run(transaction -> {
        long currentTime = System.currentTimeMillis();
        String metadataJson = SpannerMetadataStorage.GSON.toJson(metadata);

        Mutation mutation = Mutation.newInsertOrUpdateBuilder(METADATA_TABLE)
          .set(Tables.Metadata.METADATA_ID_FIELD).to(toMetadataId(entity))
          .set(Tables.Metadata.NAMESPACE_FIELD).to(entity.getValue(MetadataEntity.NAMESPACE))
          .set(Tables.Metadata.TYPE_FIELD).to(entity.getType())
          .set(Tables.Metadata.NAME_FIELD).to(entity.getValue(entity.getType()))
          .set(Tables.Metadata.CREATED_FIELD).to(currentTime)
          .set(Tables.Metadata.USER_FIELD).to(metadata.getTags().stream()
                                                .filter(s -> false).map(ScopedName::getName)
                                                .collect(Collectors.joining(",")))
          .set(Tables.Metadata.SYSTEM_FIELD).to(metadata.getTags().stream()
                                                  .filter(s -> false)
                                                  .map(ScopedName::getName)
                                                  .collect(Collectors.joining(",")))
          .set(Tables.Metadata.METADATA_COLUMN_FIELD).to(metadataJson)
          .set(Tables.Metadata.VERSION).to(version)
          .build();
        transaction.buffer(mutation);
        return null; // The callable must return something, null is fine for side effects
      });
    } catch (Exception e) {
      throw new IOException("Failed to setup initial metadata", e);
    }
  }

  /**
   * Tests the `createIndex` method.
   * Purpose: Verify that the necessary tables (`metadata`, `metadata_props`) and search indexes
   * (`UserNgramIndex`, `SystemNgramIndex`, `TextNgramIndex`, `ValueNgramIndex`) are created in Spanner.
   * This test will explicitly call `createIndex` and then query the `INFORMATION_SCHEMA` using `dbClient`
   * to verify table and index existence.
   */
  @Test
  public void testCreateIndex() {
    try (ReadOnlyTransaction tx = dbClient.readOnlyTransaction()) {
      Set<String> tables = new HashSet<>();
      Statement combinedTableStatement = Statement.newBuilder(
          "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            + "WHERE TABLE_SCHEMA = '' AND TABLE_NAME IN (@metadataTable, @metadataPropsTable)")
        .bind("metadataTable").to(METADATA_TABLE)
        .bind("metadataPropsTable").to(METADATA_PROPS_TABLE)
        .build();

      try (ResultSet resultSet = tx.executeQuery(combinedTableStatement)) {
        while (resultSet.next()) {
          String tableName = resultSet.getString(0);
          tables.add(tableName);
        }
      }
      assertTrue("Metadata table should exist", tables.contains(METADATA_TABLE));
      assertTrue("Metadata properties table should exist", tables.contains(METADATA_PROPS_TABLE));

      Set<String> metadataTableIndexes = new HashSet<>();
      Set<String> metadataPropsTableIndexes = new HashSet<>();
      Statement combinedIndexStatement = Statement.newBuilder(
          "SELECT TABLE_NAME, INDEX_NAME FROM INFORMATION_SCHEMA.INDEXES "
            + "WHERE TABLE_SCHEMA = '' AND TABLE_NAME IN (@metadataTable, @metadataPropsTable)")
        .bind("metadataTable").to(METADATA_TABLE)
        .bind("metadataPropsTable").to(METADATA_PROPS_TABLE)
        .build();

      try (ResultSet resultSet = tx.executeQuery(combinedIndexStatement)) {
        while (resultSet.next()) {
          String tableName = resultSet.getString("TABLE_NAME");
          String indexName = resultSet.getString("INDEX_NAME");

          if (METADATA_TABLE.equals(tableName)) {
            metadataTableIndexes.add(indexName);
          } else if (METADATA_PROPS_TABLE.equals(tableName)) {
            metadataPropsTableIndexes.add(indexName);
          }
        }
      }

      assertTrue("UserNgramIndex should exist", metadataTableIndexes.contains("UserNgramIndex"));
      assertTrue("SystemNgramIndex should exist", metadataTableIndexes.contains("SystemNgramIndex"));
      assertTrue("TextNgramIndex should exist", metadataTableIndexes.contains("TextNgramIndex"));
      assertTrue("ValueNgramIndex should exist", metadataPropsTableIndexes.contains("ValueNgramIndex"));
    }
  }

  /**
   * Tests that reading metadata for an existing entity retrieves the correct data and version.
   */
  @Test
  public void testReadVersionedMetadata_existingEntity() throws IOException {
    MetadataEntity existingEntity = MetadataEntity.builder()
      .append(MetadataEntity.NAMESPACE, "default")
      .appendAsType(MetadataEntity.DATASET, "my_dataset_for_read_test")
      .build();

    Metadata expectedMetadata = new Metadata(
      Collections.singleton(new ScopedName(MetadataScope.USER, "read_tag")),
      Collections.singletonMap(new ScopedName(MetadataScope.USER, "read_prop"), "read_value")
    );
    long expectedVersion = 10L;

    // Pre-populate the database with test data
    simulateInitialMetadata(existingEntity, expectedMetadata, expectedVersion);

    VersionedMetadata actualVersionedMetadata;
    try (ReadOnlyTransaction readOnlyTx = dbClient.readOnlyTransaction()) {
      actualVersionedMetadata = spannerMetadataStorage.readVersionedMetadata(existingEntity, readOnlyTx);
    }

    assertNotNull("Returned VersionedMetadata should not be null", actualVersionedMetadata);
    assertNotEquals("Returned VersionedMetadata should not be NONE", VersionedMetadata.NONE,
                    actualVersionedMetadata);
    assertEquals("Metadata tags should match", expectedMetadata.getTags(),
                 actualVersionedMetadata.getMetadata().getTags());
    assertEquals("Metadata properties should match", expectedMetadata.getProperties(),
                 actualVersionedMetadata.getMetadata().getProperties());
    assertEquals("Version should match", expectedVersion, (long) actualVersionedMetadata.getVersion());
  }

  /**
   * Tests that reading metadata for a non-existent entity correctly returns VersionedMetadata.NONE.
   */
  @Test
  public void testReadVersionedMetadata_nonExistentEntity() throws IOException {
    MetadataEntity nonExistentEntity = MetadataEntity.builder()
      .append(MetadataEntity.NAMESPACE, "default")
      .appendAsType("application", "non_existent_app")
      .build();

    VersionedMetadata actualVersionedMetadata;
    try (ReadOnlyTransaction readOnlyTx = dbClient.readOnlyTransaction()) {
      actualVersionedMetadata = spannerMetadataStorage.readVersionedMetadata(nonExistentEntity, readOnlyTx);
    }

    assertNotNull("Returned VersionedMetadata should not be null", actualVersionedMetadata);
    assertEquals("Expected VersionedMetadata.NONE for a non-existent entity",
                 VersionedMetadata.NONE, actualVersionedMetadata);
  }

  private static final class MockMetadataStorageContext implements MetadataStorageContext {

    private final Map<String, String> properties;

    private MockMetadataStorageContext(Map<String, String> properties) {
      this.properties = properties;
    }

    @Override
    public Map<String, String> getProperties() {
      return properties;
    }
  }
}

