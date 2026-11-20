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

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.cdap.cdap.spi.data.StorageProvider;
import io.cdap.cdap.spi.data.StorageProviderContext;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link StorageProvider} implementation that uses Google Cloud Spanner as the storage engine.
 * Set the following CDAP configurations to enable using Spanner.
 *
 * <ul>
 *   <li>data.storage.implementation: "gcp-spanner"</li>
 *   <li>data.storage.properties.gcp-spanner.project: &lt;GCP project name for the Spanner&gt;</li>
 *   <li>data.storage.properties.gcp-spanner.instance: &lt;Spanner instance name&gt;</li>
 *   <li>data.storage.properties.gcp-spanner.database: &lt;Spanner database name&gt;</li>
 * </ul>>
 *
 * <p>Optional configuration
 * <ul>
 *   <li>data.storage.properties.gcp-spanner.credentials.path: &lt;GCP service account file&gt;</li>
 *   <li>data.storage.properties.gcp-spanner.endpoint: &lt;Spanner API endpoint URL, including the
 *   scheme&gt;. If unset, the client library default, https://spanner.googleapis.com, is used. The
 *   Spanner messaging service reads the same property.</li>
 * </ul>
 */
public class SpannerStorageProvider implements StorageProvider {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerStorageProvider.class);

  static final String PROJECT = "project";
  static final String INSTANCE = "instance";
  static final String DATABASE = "database";
  static final String CREDENTIALS_PATH = "credentials.path";
  static final String ENDPOINT = "endpoint";
  // Environment variable that points the Spanner client library at the Spanner emulator.
  static final String SPANNER_EMULATOR_HOST = "SPANNER_EMULATOR_HOST";
  public static final String COMPRESSION_CONFIG = "compression.config";
  static final String NAME = "gcp-spanner";

  private static final String PARTITIONED_DML_TIMEOUT_HOURS = "partitioned.dml.timeout.hours";
  private static final Duration DEFAULT_PARTITIONED_DML_TIMEOUT = Duration.ofHours(12);

  private Spanner spanner;
  private SpannerStructuredTableAdmin admin;
  private RetryingSpannerTransactionRunner txRunner;

  @Override
  public void initialize(StorageProviderContext context) throws Exception {
    Map<String, String> conf = context.getConfiguration();

    String project = conf.get(PROJECT);
    if (project == null) {
      throw new IllegalArgumentException("Missing configuration " + PROJECT);
    }

    String instance = conf.get(INSTANCE);
    if (instance == null) {
      throw new IllegalArgumentException("Missing configuration " + INSTANCE);
    }

    String database = conf.get(DATABASE);
    if (database == null) {
      throw new IllegalArgumentException("Missing configuration " + DATABASE);
    }

    SpannerOptions.Builder builder = SpannerOptions.newBuilder().setProjectId(project);
    Duration partitionedDmlTimeout = DEFAULT_PARTITIONED_DML_TIMEOUT;
    String timeoutStr = conf.get(PARTITIONED_DML_TIMEOUT_HOURS);
    if (timeoutStr != null) {
      partitionedDmlTimeout = Duration.ofHours(Long.parseLong(timeoutStr));
    }
    builder.setPartitionedDmlTimeoutDuration(partitionedDmlTimeout);

    String credentialsPath = conf.get(CREDENTIALS_PATH);
    if (credentialsPath != null) {
      try (InputStream is = new FileInputStream(credentialsPath)) {
        builder.setCredentials(ServiceAccountCredentials.fromStream(is));
      }
    }

    // SpannerOptions.Builder#setHost clears the emulator host, which the builder reads from the
    // SPANNER_EMULATOR_HOST environment variable, so the endpoint is only set without an emulator.
    String endpoint = getEndpoint(conf, System.getenv(SPANNER_EMULATOR_HOST));
    if (endpoint != null) {
      builder.setHost(endpoint);
    }

    SpannerOptions options = builder.build();
    DatabaseId databaseId = DatabaseId.of(InstanceId.of(options.getProjectId(), instance),
        database);
    LOG.info("Spanner storage provider uses endpoint {} for database {}", options.getHost(),
        databaseId);

    this.spanner = options.getService();
    this.admin = new SpannerStructuredTableAdmin(spanner, databaseId, conf.get(COMPRESSION_CONFIG));
    this.txRunner = new RetryingSpannerTransactionRunner(conf, admin);
  }

  /**
   * Returns the Spanner API endpoint to set on the client, from the {@link #ENDPOINT} property.
   * Returns {@code null} if the property is unset or empty, or if {@code emulatorHost} is set.
   */
  @VisibleForTesting
  @Nullable
  static String getEndpoint(Map<String, String> conf, @Nullable String emulatorHost) {
    return Strings.isNullOrEmpty(emulatorHost) ? Strings.emptyToNull(conf.get(ENDPOINT)) : null;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public StructuredTableAdmin getStructuredTableAdmin() {
    return admin;
  }

  @Override
  public TransactionRunner getTransactionRunner() {
    return txRunner;
  }

  @Override
  public void close() {
    if (spanner != null) {
      spanner.close();
    }
  }
}
