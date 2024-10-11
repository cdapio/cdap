package io.cdap.cdap.messaging;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import io.cdap.cdap.common.conf.CConfiguration;

public class SpannerUtil {

  public static DatabaseClient getSpannerDbClient(CConfiguration cConf, Spanner spanner) {
    String projectID = cConf.get("data.storage.properties.gcp-spanner.project");
    String instanceID = getInstanceID(cConf);
    String databaseID = getDatabaseID(cConf);
    DatabaseId db = DatabaseId.of(projectID, instanceID, databaseID);
    return spanner.getDatabaseClient(db);
  }

  public static DatabaseAdminClient getSpannerDbAdminClient(Spanner spanner) {
    return spanner.getDatabaseAdminClient();
  }

  public static String getInstanceID(CConfiguration cConf) {
    return cConf.get("data.storage.properties.gcp-spanner.instance");
  }

  public static String getDatabaseID(CConfiguration cConf) {
    return cConf.get("data.storage.properties.gcp-spanner.database");
  }

  public static Spanner getSpannerService(CConfiguration cConf) {
    String projectID = cConf.get("data.storage.properties.gcp-spanner.project");
    return SpannerOptions.newBuilder().setProjectId(projectID).build().getService();
  }
}
