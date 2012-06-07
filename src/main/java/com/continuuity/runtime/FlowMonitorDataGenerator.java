package com.continuuity.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.options.Option;
import com.continuuity.common.options.OptionsParser;
import com.continuuity.metrics.service.FlowMonitorClient;
import com.continuuity.metrics.stubs.FlowMetric;
import com.continuuity.observer.StateChangeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

/**
 * A Test client for generating data - nothing fancy here.
 * <p/>
 * WARNING: This works only for HSQL. We need to change it to be generic.
 */
public class FlowMonitorDataGenerator {
  private static final Logger Log = LoggerFactory.getLogger(FlowMonitorDataGenerator.class);

  @Option(usage = "Specifies the zookeeper ensemble to connect to (Default: 127.0.0.1:2181)")
  private String zookeeper = null;

  @Option(name = "genmetrics", usage = " Specifies whether to generate flow metrics. (Default : false)")
  private boolean genMetrics = false;

  @Option(name = "clearstate", usage = "Specifies whether to clear flow state table or no before populating it")
  private boolean clearStateTable = true;

  private Connection connection;

  public void doMain(String args[]) throws Exception {
    try {
      Class.forName("org.hsqldb.jdbcDriver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    connection = DriverManager.getConnection("jdbc:hsqldb:file:/tmp/data/flowmonitordb", "sa", "");

    OptionsParser.init(this, args, System.out);
    try {
      CConfiguration conf = CConfiguration.create();
      if (zookeeper != null) {
        conf.set(Constants.CFG_ZOOKEEPER_ENSEMBLE, zookeeper);
      } else {
        conf.set(Constants.CFG_ZOOKEEPER_ENSEMBLE, Constants.DEFAULT_ZOOKEEPER_ENSEMBLE);
      }
      FlowMonitorClient client = new FlowMonitorClient(conf);

      createTable();

      /** Add Flows */
      if (clearStateTable) {
        clearFlowStateTable();
      }
      populateStateTable();

      if (genMetrics) {
        generateMetrics(client);
        System.exit(0);
      }
      client.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    connection.close();

  }

  public void addPointToFlowStateTable(int timestamp, String accountId, String application, String flow, String payload,
                                       StateChangeType type) throws Exception {
    String sql = "INSERT INTO " +
      "flow_state (timestamp, account, application, flow, payload, state) " +
      " VALUES (?, ?, ?, ?, ?, ?);";

    try {
      PreparedStatement stmt = connection.prepareStatement(sql);
      stmt.setLong(1, timestamp);
      stmt.setString(2, accountId);
      stmt.setString(3, application);
      stmt.setString(4, flow);
      stmt.setString(5, payload);
      stmt.setInt(6, type.getType());
      stmt.executeUpdate();
    } catch (SQLException e) {
      Log.error("Failed to write the state change to SQL DB (state : {}). Reason : {}", accountId, e.getMessage());
    }
  }

  public void generateMetrics(FlowMonitorClient client) {
    String[] accountIds = {"demo"};
    String[] rids = {"ABC", "XYZ", "UVW", "EFG"};
    String[] applications = {"personalization"};
    String[] versions = {"18", "28"};
    String[] flows = {"targetting", "smart-explorer", "indexer", "hustler"};
    String[] flowlets = {"flowlet-A", "flowlet-B", "flowlet-C", "flowlet-D"};
    String[] metrics = {"in", "out", "ops"};
    String[] instances = {"1", "2"};

    Random random = new Random();
    for (int i = 0; i < 100; ++i) {
      String rid = rids[(i % rids.length)];
      for (int i1 = 0; i1 < accountIds.length; ++i1) {
        int timestamp = (int) (System.currentTimeMillis() / 1000);
        for (int i2 = 0; i2 < applications.length; ++i2) {
          for (int i3 = 0; i3 < versions.length; ++i3) {
            for (int i4 = 0; i4 < flows.length; ++i4) {
              for (int i5 = 0; i5 < flowlets.length; ++i5) {
                for (int i6 = 0; i6 < instances.length; ++i6) {
                  for (int i7 = 0; i7 < metrics.length; ++i7) {
                    FlowMetric metric = new FlowMetric();
                    metric.setInstance(instances[i6]);
                    metric.setFlowlet(flowlets[i5]);
                    metric.setFlow(flows[i4]);
                    metric.setVersion(versions[i3]);
                    metric.setApplication(applications[i2]);
                    metric.setAccountId(accountIds[i1]);
                    metric.setRid(rid);
                    metric.setMetric(metrics[i7]);
                    metric.setTimestamp(timestamp);
                    metric.setValue(1000 * random.nextInt());
                    client.add(metric);
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  private void clearFlowStateTable() {
    String sql = "DELETE FROM flow_state";

    try {
      PreparedStatement stmt = connection.prepareStatement(sql);
      stmt.executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public void createTable() {
    try {
      connection.prepareStatement(
        "CREATE TABLE flow_state ( timestamp INTEGER, account VARCHAR, application VARCHAR, flow VARCHAR, " +
          " payload VARCHAR, state INTEGER)"
      ).execute();
    } catch (SQLException e) {
    }
  }

  private void populateStateTable() throws Exception {
    addPointToFlowStateTable(1, "demo", "ABC", "targetting", "", StateChangeType.DEPLOYED);
    addPointToFlowStateTable(2, "demo", "XYZ", "targetting", "", StateChangeType.DEPLOYED);
    addPointToFlowStateTable(2, "demo", "UVW", "smart-explorer", "", StateChangeType.DEPLOYED);
    addPointToFlowStateTable(2, "demo", "EFG", "hustler", "", StateChangeType.DEPLOYED);

    addPointToFlowStateTable(3, "demo", "ABC", "targetting", "", StateChangeType.STARTING);
    addPointToFlowStateTable(4, "demo", "ABC", "targetting", "", StateChangeType.STARTED);
    addPointToFlowStateTable(5, "demo", "ABC", "targetting", "", StateChangeType.STOPPED);

    addPointToFlowStateTable(6, "demo", "ABC", "targetting", "", StateChangeType.STARTING);
    addPointToFlowStateTable(7, "demo", "ABC", "targetting", "", StateChangeType.STARTED);
    addPointToFlowStateTable(8, "demo", "ABC", "targetting", "", StateChangeType.STOPPED);

    addPointToFlowStateTable(9, "demo", "XYZ", "targetting", "", StateChangeType.STARTING);
    addPointToFlowStateTable(10, "demo", "XYZ", "targetting", "", StateChangeType.STARTED);
    addPointToFlowStateTable(11, "demo", "XYZ", "targetting", "", StateChangeType.RUNNING);
    addPointToFlowStateTable(12, "demo", "XYZ", "targetting", "", StateChangeType.STOPPING);
    addPointToFlowStateTable(13, "demo", "XYZ", "targetting", "", StateChangeType.STOPPED);

    addPointToFlowStateTable(14, "demo", "UVW", "smart-explorer", "", StateChangeType.STARTING);
    addPointToFlowStateTable(15, "demo", "UVW", "smart-explorer", "", StateChangeType.STARTED);
    addPointToFlowStateTable(16, "demo", "UVW", "smart-explorer", "", StateChangeType.RUNNING);

    addPointToFlowStateTable(17, "demo", "EFG", "hustler", "", StateChangeType.STARTING);
    addPointToFlowStateTable(18, "demo", "EFG", "hustler", "", StateChangeType.FAILED);
  }

  public static void main(String[] args) {
    FlowMonitorDataGenerator dg = new FlowMonitorDataGenerator();
    try {
      dg.doMain(args);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
