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

import io.cdap.cdap.api.metrics.MetricsCollector;
import io.cdap.cdap.spi.data.StorageProviderContext;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.StructuredTableTest;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;

/**
 * Unit tests for GCP spanner implementation of the {@link StructuredTable}. This test needs the following
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
public class SpannerStructuredTableTest extends StructuredTableTest {

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
    configs.put(SpannerStorageProvider.PROJECT, project);
    configs.put(SpannerStorageProvider.INSTANCE, instance);
    configs.put(SpannerStorageProvider.DATABASE, database);

    if (credentialsPath != null) {
      configs.put(SpannerStorageProvider.CREDENTIALS_PATH, credentialsPath);
    }

    StorageProviderContext context = new MockStorageProviderContext(configs);

    storageProvider = new SpannerStorageProvider();
    storageProvider.initialize(context);
  }

  @AfterClass
  public static void closeSpannerStorageProvider() {
    Optional.ofNullable(storageProvider).ifPresent(SpannerStorageProvider::close);
  }

  @Override
  @Ignore
  public void testSortedPrimaryKeyFilteredIndexScan() {
    // no implementation
  }

  @Override
  protected StructuredTableAdmin getStructuredTableAdmin() {
    return storageProvider.getStructuredTableAdmin();
  }

  @Override
  protected TransactionRunner getTransactionRunner() {
    return storageProvider.getTransactionRunner();
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
