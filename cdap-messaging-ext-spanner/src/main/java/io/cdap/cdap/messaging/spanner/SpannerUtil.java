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
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Utility class for spanner messaging service.
 */
class SpannerUtil {

  static final String PROJECT = "project";
  static final String INSTANCE = "instance";
  static final String DATABASE = "database";
  static final String CREDENTIALS_PATH = "credentials.path";
  static final String PUBLISH_BATCH_SIZE = "publish.batch.size";
  static final String PUBLISH_BATCH_TIMEOUT_MILLIS = "publish.batch.timeout.millis";
  static final String PUBLISH_DELAY_MILLIS = "publish.delay.millis";

  static DatabaseClient getSpannerDbClient(String projectID, String instanceID,
      String databaseID, Spanner spanner) {
    DatabaseId db = DatabaseId.of(projectID, instanceID, databaseID);
    return spanner.getDatabaseClient(db);
  }

  static DatabaseAdminClient getSpannerDbAdminClient(Spanner spanner) {
    return spanner.getDatabaseAdminClient();
  }

  static String getInstanceID(Map<String, String> cConf) {
    String instance = cConf.get(INSTANCE);
    if (instance == null) {
      throw new IllegalArgumentException("Missing configuration " + PROJECT);
    }
    return instance;
  }

  static String getDatabaseID(Map<String, String> cConf) {
    String instance = cConf.get(DATABASE);
    if (instance == null) {
      throw new IllegalArgumentException("Missing configuration " + DATABASE);
    }
    return cConf.get(DATABASE);
  }

  static String getProjectID(Map<String, String> cConf) {
    String instance = cConf.get(PROJECT);
    if (instance == null) {
      throw new IllegalArgumentException("Missing configuration " + PROJECT);
    }
    return cConf.get(PROJECT);
  }

  static Credentials getCredentials(Map<String, String> cConf) throws IOException {
    String credentialsPath = cConf.get(CREDENTIALS_PATH);
    if (credentialsPath != null) {
      try (InputStream is = Files.newInputStream(Paths.get(credentialsPath))) {
        return ServiceAccountCredentials.fromStream(is);
      }
    }
    return null;
  }

  public static Spanner getSpannerService(String projectID, Credentials credentials) {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder().setProjectId(projectID);
    if (credentials != null) {
      builder.setCredentials(credentials);
    }

    return builder.build().getService();
  }
}