/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.storage.spanner;

import static io.cdap.cdap.storage.spanner.SpannerStructuredTableAdmin.convertSpecToCompatibleSchema;

import io.cdap.cdap.api.metrics.MetricsCollector;
import io.cdap.cdap.spi.data.StorageProviderContext;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.StructuredTableAdminTest;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.table.StructuredTableSchema;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for GCP spanner implementation of the {@link StructuredTableAdmin}. This test needs the following
 * Java properties to run. If they are not provided, tests will be ignored.
 *
 * <ul>
 *   <li>gcp.project - GCP project name</li>
 *   <li>gcp.spanner.instance - GCP spanner instance name</li>
 *   <li>gcp.spanner.database - GCP spanner database name</li>
 *   <li>(optional) gcp.credentials.path - Local file path to the service account
 *   json that has the "Cloud Spanner Database User" role</li>
 * </ul>
 */
public class SpannerStructuredTableAdminTest extends StructuredTableAdminTest {

  private static SpannerStorageProvider storageProvider;

  @BeforeClass
  public static void createSpannerStorageProvider() throws Exception {
    String project = System.getProperty("gcp.project");
    String instance = System.getProperty("gcp.spanner.instance");
    String database = System.getProperty("gcp.spanner.database");
    String credentialsPath = System.getProperty("gcp.credentials.path");

    // GCP project, instance, and database must be provided
    Assume.assumeNotNull(project, instance, database);

    Map<String, String> configs = new HashMap<>();
    if (credentialsPath != null) {
      configs.put(SpannerStorageProvider.CREDENTIALS_PATH, credentialsPath);
    }
    configs.put(SpannerStorageProvider.PROJECT, project);
    configs.put(SpannerStorageProvider.INSTANCE, instance);
    configs.put(SpannerStorageProvider.DATABASE, database);

    StorageProviderContext context = new MockStorageProviderContext(configs);

    storageProvider = new SpannerStorageProvider();
    storageProvider.initialize(context);
  }

  @AfterClass
  public static void closeSpannerStorageProvider() {
    Optional.ofNullable(storageProvider).ifPresent(SpannerStorageProvider::close);
  }

  @Override
  protected StructuredTableAdmin getStructuredTableAdmin() {
    return storageProvider.getStructuredTableAdmin();
  }

  @Test
  @Override
  public void testAdmin() throws Exception {
    StructuredTableAdmin admin = getStructuredTableAdmin();

    // Assert SIMPLE_TABLE Empty
    Assert.assertFalse(admin.exists(SIMPLE_TABLE));

    // getSchema SIMPLE_TABLE should fail
    try {
      admin.getSchema(SIMPLE_TABLE);
      Assert.fail("Expected getSchema SIMPLE_TABLE to fail");
    } catch (TableNotFoundException e) {
      // Expected
    }

    // Create SIMPLE_TABLE
    admin.createOrUpdate(SIMPLE_TABLE_SPEC);
    Assert.assertTrue(admin.exists(SIMPLE_TABLE));

    // Assert SIMPLE_TABLE schema: checking equality after compatible conversion because of INT/LONG
    // to INT64 conversion in Spanner
    StructuredTableSchema simpleTableSchema = admin.getSchema(SIMPLE_TABLE);
    Assert.assertEquals(simpleTableSchema, convertSpecToCompatibleSchema(SIMPLE_TABLE_SPEC));

    // Update SIMPLE_TABLE spec to UPDATED_SIMPLE_TABLE_SPEC
    admin.createOrUpdate(UPDATED_SIMPLE_TABLE_SPEC);

    // Assert UPDATED_SIMPLE_TABLE_SPEC schema
    StructuredTableSchema updateSimpleTableSchema = admin.getSchema(SIMPLE_TABLE);
    Assert.assertEquals(
        updateSimpleTableSchema, convertSpecToCompatibleSchema(UPDATED_SIMPLE_TABLE_SPEC));
  }

  @Test
  @Override
  public void testCreateOrUpdateTwiceShouldSucceed() throws Exception {
    StructuredTableAdmin admin = getStructuredTableAdmin();

    // Assert SIMPLE_TABLE Empty
    Assert.assertFalse(admin.exists(SIMPLE_TABLE));

    // Calling to createOrUpdate the same SIMPLE_TABLE spec twice to mimic the scenario of
    // connecting to an exsting DB and make sure the second time passes the equality check after
    // schema compatibility conversion
    admin.createOrUpdate(SIMPLE_TABLE_SPEC);
    admin.createOrUpdate(SIMPLE_TABLE_SPEC);
    Assert.assertTrue(admin.exists(SIMPLE_TABLE));

    // Assert SIMPLE_TABLE schema
    StructuredTableSchema simpleTableSchema = admin.getSchema(SIMPLE_TABLE);
    Assert.assertEquals(simpleTableSchema, convertSpecToCompatibleSchema(SIMPLE_TABLE_SPEC));
  }

  @Test
  @Override
  public void testInconsistentKeyOrderInSchema() throws Exception {
    StructuredTableAdmin admin = getStructuredTableAdmin();

    // Assert INCONSISTENT_PRIMARY_KEY_TABLE Empty
    Assert.assertFalse(admin.exists(INCONSISTENT_PRIMARY_KEY_TABLE));

    // Create INCONSISTENT_PRIMARY_KEY_TABLE
    admin.createOrUpdate(INCONSISTENT_PRIMARY_KEY_TABLE_SPEC);
    Assert.assertTrue(admin.exists(INCONSISTENT_PRIMARY_KEY_TABLE));

    // Assert INCONSISTENT_PRIMARY_KEY_TABLE schema
    StructuredTableSchema tableSchema = admin.getSchema(INCONSISTENT_PRIMARY_KEY_TABLE);
    // Checking equality after compatible conversion because of INT/LONG to INT64 conversion in
    // Spanner
    Assert.assertEquals(
        tableSchema, convertSpecToCompatibleSchema(INCONSISTENT_PRIMARY_KEY_TABLE_SPEC));
  }

  private static final class MockStorageProviderContext implements StorageProviderContext {

    private final Map<String, String> config;

    MockStorageProviderContext(Map<String, String> config) {
      this.config = config;
    }

    @Override
    public MetricsCollector getMetricsCollector() {
      return new MetricsCollector() {
        @Override
        public void increment(String metricName, long value) {
          // no-op
        }

        @Override
        public void gauge(String metricName, long value) {
          // no-op
        }
      };
    }

    @Override
    public Map<String, String> getConfiguration() {
      return config;
    }

    @Override
    public Map<String, String> getSecurityConfiguration() {
      return Collections.emptyMap();
    }
  }
}
