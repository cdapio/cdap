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

package io.cdap.cdap.messaging;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for spanner messaging service.
 */
public class SpannerUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerUtil.class);


  public static DatabaseClient getSpannerDbClient(Map<String, String> cConf, Spanner spanner) {
    String projectID = cConf.get("project");
    String instanceID = getInstanceID(cConf);
    String databaseID = getDatabaseID(cConf);
    DatabaseId db = DatabaseId.of(projectID, instanceID, databaseID);
    return spanner.getDatabaseClient(db);
  }

  public static DatabaseAdminClient getSpannerDbAdminClient(Spanner spanner) {
    return spanner.getDatabaseAdminClient();
  }

  public static String getInstanceID(Map<String, String> cConf) {
    return cConf.get("instance");
  }

  public static String getDatabaseID(Map<String, String> cConf) {
    return cConf.get("database");
  }

  public static Spanner getSpannerService(Map<String, String> cConf) {
    String projectID = cConf.get("project");
    LOG.info("projectID {}", projectID);
    return SpannerOptions.newBuilder().setProjectId(projectID).build().getService();
  }
}
