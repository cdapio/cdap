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
import io.cdap.cdap.spi.data.StorageProvider;
import io.cdap.cdap.spi.data.StorageProviderContext;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;

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
 * Optional configuration
 * <ul>
 *   <li>data.storage.properties.gcp-spanner.credentials.path: &lt;GCP service account file&gt;</li>
 * </ul>
 */
public class SpannerStorageProvider implements StorageProvider {

  static final String PROJECT = "project";
  static final String INSTANCE = "instance";
  static final String DATABASE = "database";
  static final String CREDENTIALS_PATH = "credentials.path";

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

    String credentialsPath = conf.get(CREDENTIALS_PATH);
    if (credentialsPath != null) {
      try (InputStream is = new FileInputStream(credentialsPath)) {
        builder.setCredentials(ServiceAccountCredentials.fromStream(is));
      }
    }

    SpannerOptions options = builder.build();
    DatabaseId databaseId = DatabaseId.of(InstanceId.of(options.getProjectId(), instance),
        database);

    this.spanner = options.getService();
    this.admin = new SpannerStructuredTableAdmin(spanner, databaseId);
    this.txRunner = new RetryingSpannerTransactionRunner(conf, admin);
  }

  @Override
  public String getName() {
    return "gcp-spanner";
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
