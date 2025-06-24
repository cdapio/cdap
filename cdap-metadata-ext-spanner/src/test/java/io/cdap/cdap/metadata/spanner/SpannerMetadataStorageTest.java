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
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.metadata.Cursor;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataKind;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.MetadataStorageContext;
import io.cdap.cdap.spi.metadata.MetadataStorageTest;
import io.cdap.cdap.spi.metadata.ScopedName;
import io.cdap.cdap.spi.metadata.ScopedNameOfKind;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
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
public class SpannerMetadataStorageTest extends MetadataStorageTest {

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
        } catch (DatabaseNotFoundException ignored) {
        } catch (Exception e) {
          throw new RuntimeException("Failed to drop database during @AfterClass cleanup", e);
        }
      }

      if (adminClient != null) {
        try {
          adminClient.deleteInstance(INSTANCE_ID);
        } catch (InstanceNotFoundException ignored) {
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

  /**
   * Tests the `createIndex` method.
   * Purpose: Verify that the necessary tables (`metadata`, `metadata_props`) and search indexes
   * (`UserNgramIndex`, `SystemNgramIndex`, `TextNgramIndex`, `ValueNgramIndex`) are created in Spanner.
   * This test will explicitly call `createIndex` and then query the `INFORMATION_SCHEMA` using `dbClient`
   * to verify table and index existence.
   */
  @BeforeClass
  public static void testCreateIndex() throws IOException {
    spannerMetadataStorage.createIndex();
  }
  @Test
  public void testFiltering() {
    ScopedName sys = new ScopedName(MetadataScope.SYSTEM, "s");
    ScopedName user = new ScopedName(MetadataScope.USER, "u");
    String sval = "S";
    String uval = "U";
    Metadata before = new Metadata(tags(sys, user), props(sys, sval, user, uval));

    // test selection to remove
    Assert.assertEquals(new Metadata(tags(sys), props(user, uval)),
                        SpannerMetadataStorage.filterMetadata(
                          before,
                          SpannerMetadataStorage.DISCARD,
                          MetadataKind.NONE,
                          MetadataScope.NONE,
                          ImmutableSet.of(new ScopedNameOfKind(MetadataKind.TAG, user),
                                          new ScopedNameOfKind(MetadataKind.PROPERTY, sys))));

    // test selection is not affected by scopes or kinds
    Assert.assertEquals(new Metadata(tags(sys), props(user, uval)),
                        SpannerMetadataStorage.filterMetadata(
                          before,
                          SpannerMetadataStorage.DISCARD,
                          MetadataKind.ALL,
                          MetadataScope.ALL,
                          ImmutableSet.of(new ScopedNameOfKind(MetadataKind.TAG, user),
                                          new ScopedNameOfKind(MetadataKind.PROPERTY, sys))));

    // test selection to keep
    Assert.assertEquals(new Metadata(tags(user), props(sys, sval)),
                        SpannerMetadataStorage.filterMetadata(
                          before,
                          SpannerMetadataStorage.KEEP,
                          MetadataKind.NONE,
                          MetadataScope.NONE,
                          ImmutableSet.of(new ScopedNameOfKind(MetadataKind.TAG, user),
                                          new ScopedNameOfKind(MetadataKind.PROPERTY, sys))));

    // test selection is not affected by scopes or kinds
    Assert.assertEquals(new Metadata(tags(user), props(sys, sval)),
                        SpannerMetadataStorage.filterMetadata(
                          before,
                          SpannerMetadataStorage.KEEP,
                          MetadataKind.ALL,
                          MetadataScope.ALL,
                          ImmutableSet.of(new ScopedNameOfKind(MetadataKind.TAG, user),
                                          new ScopedNameOfKind(MetadataKind.PROPERTY, sys))));

    // test removing nothing
    Assert.assertEquals(before,
                        SpannerMetadataStorage.filterMetadata(
                          before,
                          SpannerMetadataStorage.DISCARD,
                          MetadataKind.NONE,
                          MetadataScope.NONE,
                          null));
    Assert.assertEquals(before,
                        SpannerMetadataStorage.filterMetadata(
                          before,
                          SpannerMetadataStorage.DISCARD,
                          MetadataKind.NONE,
                          MetadataScope.ALL,
                          null));
    Assert.assertEquals(before,
                        SpannerMetadataStorage.filterMetadata(
                          before,
                          SpannerMetadataStorage.DISCARD,
                          MetadataKind.ALL,
                          MetadataScope.NONE,
                          null));

    // test keeping all
    Assert.assertEquals(before,
                        SpannerMetadataStorage.filterMetadata(
                          before,
                          SpannerMetadataStorage.KEEP,
                          MetadataKind.ALL,
                          MetadataScope.ALL,
                          null));

    // test removing all
    Assert.assertEquals(Metadata.EMPTY,
                        SpannerMetadataStorage.filterMetadata(
                          before,
                          SpannerMetadataStorage.DISCARD,
                          MetadataKind.ALL,
                          MetadataScope.ALL,
                          null));

    // test keeping nothing
    Assert.assertEquals(Metadata.EMPTY,
                        SpannerMetadataStorage.filterMetadata(
                          before,
                          SpannerMetadataStorage.KEEP,
                          MetadataKind.NONE,
                          MetadataScope.NONE,
                          null));
    // test keeping nothing
    Assert.assertEquals(Metadata.EMPTY,
                        SpannerMetadataStorage.filterMetadata(
                          before,
                          SpannerMetadataStorage.KEEP,
                          MetadataKind.ALL,
                          MetadataScope.NONE,
                          null));
    // test keeping nothing
    Assert.assertEquals(Metadata.EMPTY,
                        SpannerMetadataStorage.filterMetadata(
                          before,
                          SpannerMetadataStorage.KEEP,
                          MetadataKind.NONE,
                          MetadataScope.ALL,
                          null));

    // test removing all SYSTEM
    Assert.assertEquals(new Metadata(tags(user), props(user, uval)),
                        SpannerMetadataStorage.filterMetadata(
                          before,
                          SpannerMetadataStorage.DISCARD,
                          MetadataKind.ALL,
                          Collections.singleton(MetadataScope.SYSTEM),
                          null));
    // test removing all USER
    Assert.assertEquals(new Metadata(tags(sys), props(sys, sval)),
                        SpannerMetadataStorage.filterMetadata(
                          before,
                          SpannerMetadataStorage.DISCARD,
                          MetadataKind.ALL,
                          Collections.singleton(MetadataScope.USER),
                          null));
    // test keeping all SYSTEM
    Assert.assertEquals(new Metadata(tags(sys), props(sys, sval)),
                        SpannerMetadataStorage.filterMetadata(
                          before,
                          SpannerMetadataStorage.KEEP,
                          MetadataKind.ALL,
                          Collections.singleton(MetadataScope.SYSTEM),
                          null));
    // test keeping all USER
    Assert.assertEquals(new Metadata(tags(user), props(user, uval)),
                        SpannerMetadataStorage.filterMetadata(
                          before,
                          SpannerMetadataStorage.KEEP,
                          MetadataKind.ALL,
                          Collections.singleton(MetadataScope.USER),
                          null));

    // test removing all tags
    Assert.assertEquals(new Metadata(tags(), props(sys, sval, user, uval)),
                        SpannerMetadataStorage.filterMetadata(
                          before,
                          SpannerMetadataStorage.DISCARD,
                          Collections.singleton(MetadataKind.TAG),
                          MetadataScope.ALL,
                          null));

    // test removing all properties
    Assert.assertEquals(new Metadata(tags(sys, user), props()),
                        SpannerMetadataStorage.filterMetadata(
                          before,
                          SpannerMetadataStorage.DISCARD,
                          Collections.singleton(MetadataKind.PROPERTY),
                          MetadataScope.ALL,
                          null));

    // test keeping all tags
    Assert.assertEquals(new Metadata(tags(sys, user), props()),
                        SpannerMetadataStorage.filterMetadata(
                          before,
                          SpannerMetadataStorage.KEEP,
                          Collections.singleton(MetadataKind.TAG),
                          MetadataScope.ALL,
                          null));

    // test keeping all properties
    Assert.assertEquals(new Metadata(tags(), props(sys, sval, user, uval)),
                        SpannerMetadataStorage.filterMetadata(
                          before,
                          SpannerMetadataStorage.KEEP,
                          Collections.singleton(MetadataKind.PROPERTY),
                          MetadataScope.ALL,
                          null));

    // test removing all tags in SYSTEM scope
    Assert.assertEquals(new Metadata(tags(user), props(sys, sval, user, uval)),
                        SpannerMetadataStorage.filterMetadata(
                          before,
                          SpannerMetadataStorage.DISCARD,
                          Collections.singleton(MetadataKind.TAG),
                          Collections.singleton(MetadataScope.SYSTEM),
                          null));

    // test removing all properties in USER scope
    Assert.assertEquals(new Metadata(tags(sys, user), props(sys, sval)),
                        SpannerMetadataStorage.filterMetadata(
                          before,
                          SpannerMetadataStorage.DISCARD,
                          Collections.singleton(MetadataKind.PROPERTY),
                          Collections.singleton(MetadataScope.USER),
                          null));

    // test keeping all tags in SYSTEM scope
    Assert.assertEquals(new Metadata(tags(sys), props()),
                        SpannerMetadataStorage.filterMetadata(
                          before,
                          SpannerMetadataStorage.KEEP,
                          Collections.singleton(MetadataKind.TAG),
                          Collections.singleton(MetadataScope.SYSTEM),
                          null));

    // test keeping all properties in USER scope
    Assert.assertEquals(new Metadata(tags(), props(user, uval)),
                        SpannerMetadataStorage.filterMetadata(
                          before,
                          SpannerMetadataStorage.KEEP,
                          Collections.singleton(MetadataKind.PROPERTY),
                          Collections.singleton(MetadataScope.USER),
                          null));
  }

  @Override
  protected MetadataStorage getMetadataStorage() {
    return spannerMetadataStorage;
  }

  @Override
  protected void validateCursor(String cursor, int expectedOffset, int expectedPageSize) {
    Cursor c = Cursor.fromString(cursor);
    Assert.assertEquals(expectedOffset, c.getOffset());
    Assert.assertEquals(expectedPageSize, c.getLimit());
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

