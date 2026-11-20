/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.messaging.spanner;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for spanner messaging service.
 */
class SpannerUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerUtil.class);

  static final String PROJECT = "project";
  static final String INSTANCE = "instance";
  static final String DATABASE = "database";
  static final String CREDENTIALS_PATH = "credentials.path";
  static final String PUBLISH_BATCH_SIZE = "publish.batch.size";
  static final String PUBLISH_BATCH_TIMEOUT_MILLIS = "publish.batch.timeout.millis";
  static final String PUBLISH_DELAY_MILLIS = "publish.batch.poll.millis";
  static final String SPANNER_EMULATOR_HOST = "SPANNER_EMULATOR_HOST";
  // Optional Spanner API endpoint URL, including the scheme. If unset, the client library default,
  // https://spanner.googleapis.com, is used. The messaging service context passes the gcp-spanner
  // storage properties through, so this is data.storage.properties.gcp-spanner.endpoint, the same
  // property that the Spanner storage provider reads.
  static final String ENDPOINT = "endpoint";

  static DatabaseClient getSpannerDbClient(String projectId, String instanceId, String databaseId,
      Spanner spanner) {
    DatabaseId db = DatabaseId.of(projectId, instanceId, databaseId);
    return spanner.getDatabaseClient(db);
  }

  static DatabaseAdminClient getSpannerDbAdminClient(Spanner spanner) {
    return spanner.getDatabaseAdminClient();
  }

  static String getInstanceId(Map<String, String> conf) {
    String instance = conf.get(INSTANCE);
    if (instance == null) {
      throw new IllegalArgumentException("Missing configuration " + INSTANCE);
    }
    return instance;
  }

  static String getDatabaseId(Map<String, String> conf) {
    String database = conf.get(DATABASE);
    if (database == null) {
      throw new IllegalArgumentException("Missing configuration " + DATABASE);
    }
    return database;
  }

  static String getProjectId(Map<String, String> conf) {
    String project = conf.get(PROJECT);
    if (project == null) {
      throw new IllegalArgumentException("Missing configuration " + PROJECT);
    }
    return project;
  }

  static Credentials getCredentials(Map<String, String> conf) throws IOException {
    String credentialsPath = conf.get(CREDENTIALS_PATH);
    if (credentialsPath != null) {
      try (InputStream is = Files.newInputStream(Paths.get(credentialsPath))) {
        return ServiceAccountCredentials.fromStream(is);
      }
    }
    return null;
  }

  /**
   * Obtains a Spanner service client for the Spanner API endpoint in the {@link #ENDPOINT}
   * property, if set. If the {@code SPANNER_EMULATOR_HOST} property or environment variable is set,
   * the client connects to the emulator instead.
   */
  public static Spanner getSpannerService(Map<String, String> conf, String projectId,
      @Nullable Credentials credentials) {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder().setProjectId(projectId);

    String emulatorHost = conf.get(SPANNER_EMULATOR_HOST);
    if (!Strings.isNullOrEmpty(emulatorHost)) {
      builder.setEmulatorHost(emulatorHost);
      // CRITICAL: For the emulator, explicitly set credentials to null.
      // This prevents the client library from attempting to find/validate
      // default credentials (which causes UNAUTHENTICATED/PERMISSION_DENIED with emulator).
      builder.setCredentials(NoCredentials.getInstance());
      LOG.trace("Connecting to Spanner Emulator at {}", emulatorHost);
    } else if (credentials != null) {
      // If not in emulator mode AND real credentials are provided, use them.
      builder.setCredentials(credentials);
      LOG.trace("Connecting to production Spanner instance using provided credentials.");
    } else {
      // If not in emulator mode AND credentials are null, the builder will attempt
      // to find Application Default Credentials (ADC) or other default mechanisms.
      LOG.trace(
          "Connecting to production Spanner instance using Application Default Credentials (or defaults).");
    }

    // SpannerOptions.Builder#setHost clears the emulator host, whether set above or read by the
    // builder from the SPANNER_EMULATOR_HOST environment variable, so the endpoint is only set
    // without an emulator.
    String endpoint = getEndpoint(conf, System.getenv(SPANNER_EMULATOR_HOST));
    if (endpoint != null) {
      builder.setHost(endpoint);
    }

    SpannerOptions options = builder.build();
    LOG.info("Spanner messaging service uses endpoint {}", options.getHost());
    return options.getService();
  }

  /**
   * Returns the Spanner API endpoint to set on the client, from the {@link #ENDPOINT} property.
   * Returns {@code null} if the property is unset or empty, or if an emulator is configured
   * through the {@link #SPANNER_EMULATOR_HOST} property or {@code envEmulatorHost}.
   */
  @VisibleForTesting
  @Nullable
  static String getEndpoint(Map<String, String> conf, @Nullable String envEmulatorHost) {
    if (!Strings.isNullOrEmpty(conf.get(SPANNER_EMULATOR_HOST))
        || !Strings.isNullOrEmpty(envEmulatorHost)) {
      return null;
    }
    return Strings.emptyToNull(conf.get(ENDPOINT));
  }
}
