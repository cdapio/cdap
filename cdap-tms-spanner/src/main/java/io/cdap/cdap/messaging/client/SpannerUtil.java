/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.messaging.client;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;

public class SpannerUtil {

  public static String instanceId = "spanner-instance";

  public static String databaseId = "test-692-spanner";

 private static DatabaseClient dbClient;

  public static DatabaseClient getSpannerDbClient() {
    if (dbClient != null) {
      return dbClient;
    }

    synchronized (SpannerUtil.class) {
      if (dbClient != null) {
        return dbClient;
      }
      Spanner spanner =
          SpannerOptions.newBuilder().setProjectId("ardekani-cdf-sandbox2").build().getService();
      DatabaseId db = DatabaseId.of("ardekani-cdf-sandbox2", instanceId, databaseId);

      dbClient = spanner.getDatabaseClient(db);
    }

    return dbClient;
  }

  public static DatabaseAdminClient getSpannerDbAdminClient() {

    Spanner spanner =
        SpannerOptions.newBuilder().setProjectId("ardekani-cdf-sandbox2").build().getService();
    DatabaseId db = DatabaseId.of("ardekani-cdf-sandbox2", instanceId, databaseId);

    return spanner.getDatabaseAdminClient();
  }
}
