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


      connection.setAutoCommit(true);
      createTable();
      clearFlowStateTable();
      populateStateTable();
      Thread.sleep(10000);
      connection.commit();
      connection.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  public void addPointToFlowStateTable(int timestamp, String accountId, String appId, String runId, String flowId,
                                       String payload, StateChangeType type) throws Exception {
    String sql = "INSERT INTO " +
      "flow_state (timestamp, account, application, flow, runid, payload, state) " +
      " VALUES (?, ?, ?, ?, ?, ?, ?);";

    try {
      PreparedStatement stmt = connection.prepareStatement(sql);
      stmt.setLong(1, timestamp);
      stmt.setString(2, accountId);
      stmt.setString(3, appId);
      stmt.setString(4, flowId);
      stmt.setString(5, runId);
      stmt.setString(6, payload);
      stmt.setInt(7, type.getType());
      stmt.executeUpdate();
    } catch (SQLException e) {
      Log.error("Failed to write the state change to SQL DB (state : {}). Reason : {}", accountId, e.getMessage());
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
        "CREATE TABLE flow_state ( timestamp INTEGER, account VARCHAR, application VARCHAR, flow VARCHAR, runid VARCHAR, " +
          " payload VARCHAR, state INTEGER)"
      ).execute();
    } catch (SQLException e) {
    }
  }

  private void populateStateTable() throws Exception {
    addPointToFlowStateTable(1, "demo", "ABC", null, "targetting", "", StateChangeType.DEPLOYED);
    addPointToFlowStateTable(2, "demo", "XYZ", null, "targetting", "", StateChangeType.DEPLOYED);
    addPointToFlowStateTable(2, "demo", "UVW", null, "smart-explorer",  "", StateChangeType.DEPLOYED);
    addPointToFlowStateTable(2, "demo", "EFG", null, "hustler", "", StateChangeType.DEPLOYED);

    addPointToFlowStateTable(3, "demo", "ABC", "r1", "targetting", "", StateChangeType.STARTING);
    addPointToFlowStateTable(4, "demo", "ABC", "r1", "targetting", "", StateChangeType.STARTED);
    addPointToFlowStateTable(5, "demo", "ABC", "r1", "targetting", "", StateChangeType.STOPPED);

    addPointToFlowStateTable(6, "demo", "ABC", "r2", "targetting", "", StateChangeType.STARTING);
    addPointToFlowStateTable(7, "demo", "ABC", "r2", "targetting", "", StateChangeType.STARTED);
    addPointToFlowStateTable(8, "demo", "ABC", "r2", "targetting", "", StateChangeType.STOPPED);

    addPointToFlowStateTable(9, "demo", "XYZ", "r3", "targetting","", StateChangeType.STARTING);
    addPointToFlowStateTable(10, "demo", "XYZ", "r3", "targetting",  "", StateChangeType.STARTED);
    addPointToFlowStateTable(11, "demo", "XYZ", "r3",  "targetting",  "", StateChangeType.RUNNING);
    addPointToFlowStateTable(12, "demo", "XYZ", "r3", "targetting",  "", StateChangeType.STOPPING);
    addPointToFlowStateTable(13, "demo", "XYZ", "r3", "targetting",  "", StateChangeType.STOPPED);

    addPointToFlowStateTable(14, "demo", "UVW", "r4", "smart-explorer", "", StateChangeType.STARTING);
    addPointToFlowStateTable(15, "demo", "UVW", "r4", "smart-explorer", "", StateChangeType.STARTED);
    addPointToFlowStateTable(16, "demo", "UVW", "r4", "smart-explorer", "", StateChangeType.RUNNING);

    addPointToFlowStateTable(17, "demo", "EFG", "r5", "hustler",  "", StateChangeType.STARTING);
    addPointToFlowStateTable(18, "demo", "EFG", "r5", "hustler",  "", StateChangeType.FAILED);

    addPointToFlowStateTable(19, "demo", "XYZ", "r6", "targetting",  "", StateChangeType.STARTING);
    addPointToFlowStateTable(20, "demo", "XYZ", "r6", "targetting",  "", StateChangeType.STARTED);
    addPointToFlowStateTable(21, "demo", "XYZ", "r6", "targetting",  "", StateChangeType.RUNNING);
    addPointToFlowStateTable(22, "demo", "XYZ", "r6", "targetting", "", StateChangeType.STOPPING);
    addPointToFlowStateTable(23, "demo", "XYZ", "r6", "targetting",  "", StateChangeType.STOPPED);

    addPointToFlowStateTable(24, "demo", "XYZ", null, "targetting", "{\"meta\":{\"name\":\"test\",\"email\":\"me@continuuity.com\",\"app\":\"personalization\"},\"flowlets\":[{\"name\":\"A1\",\"classname\":\"com.continuuity.flow.flowlet.api.Flowlet\",\"instances\":1,\"resource\":{\"cpu\":1,\"memory\":512,\"uplink\":1,\"downlink\":1},\"id\":0},{\"name\":\"A2\",\"classname\":\"com.continuuity.flow.flowlet.api.Flowlet\",\"instances\":1,\"resource\":{\"cpu\":1,\"memory\":512,\"uplink\":1,\"downlink\":1},\"id\":0},{\"name\":\"A3\",\"classname\":\"com.continuuity.flow.flowlet.api.Flowlet\",\"instances\":2,\"resource\":{\"cpu\":1,\"memory\":512,\"uplink\":1,\"downlink\":1},\"id\":0}],\"connections\":[{\"from\":{\"flowlet\":\"A1\",\"stream\":\"out\"},\"to\":{\"flowlet\":\"A2\",\"stream\":\"in\"}},{\"from\":{\"flowlet\":\"A2\",\"stream\":\"out\"},\"to\":{\"flowlet\":\"A3\",\"stream\":\"in\"}}],\"streams\":{}}", StateChangeType.DEPLOYED);
    addPointToFlowStateTable(25, "demo", "ABC", null, "targetting", "{\"meta\":{\"name\":\"test\",\"email\":\"me@continuuity.com\",\"app\":\"personalization\"},\"flowlets\":[{\"name\":\"A1\",\"classname\":\"com.continuuity.flow.flowlet.api.Flowlet\",\"instances\":1,\"resource\":{\"cpu\":1,\"memory\":512,\"uplink\":1,\"downlink\":1},\"id\":0},{\"name\":\"A2\",\"classname\":\"com.continuuity.flow.flowlet.api.Flowlet\",\"instances\":1,\"resource\":{\"cpu\":1,\"memory\":512,\"uplink\":1,\"downlink\":1},\"id\":0},{\"name\":\"A3\",\"classname\":\"com.continuuity.flow.flowlet.api.Flowlet\",\"instances\":2,\"resource\":{\"cpu\":1,\"memory\":512,\"uplink\":1,\"downlink\":1},\"id\":0}],\"connections\":[{\"from\":{\"flowlet\":\"A1\",\"stream\":\"out\"},\"to\":{\"flowlet\":\"A2\",\"stream\":\"in\"}},{\"from\":{\"flowlet\":\"A2\",\"stream\":\"out\"},\"to\":{\"flowlet\":\"A3\",\"stream\":\"in\"}}],\"streams\":{}}", StateChangeType.DEPLOYED);
    addPointToFlowStateTable(26, "demo", "EFG", null, "targetting", "{\"meta\":{\"name\":\"test\",\"email\":\"me@continuuity.com\",\"app\":\"personalization\"},\"flowlets\":[{\"name\":\"A1\",\"classname\":\"com.continuuity.flow.flowlet.api.Flowlet\",\"instances\":1,\"resource\":{\"cpu\":1,\"memory\":512,\"uplink\":1,\"downlink\":1},\"id\":0},{\"name\":\"A2\",\"classname\":\"com.continuuity.flow.flowlet.api.Flowlet\",\"instances\":1,\"resource\":{\"cpu\":1,\"memory\":512,\"uplink\":1,\"downlink\":1},\"id\":0},{\"name\":\"A3\",\"classname\":\"com.continuuity.flow.flowlet.api.Flowlet\",\"instances\":2,\"resource\":{\"cpu\":1,\"memory\":512,\"uplink\":1,\"downlink\":1},\"id\":0}],\"connections\":[{\"from\":{\"flowlet\":\"A1\",\"stream\":\"out\"},\"to\":{\"flowlet\":\"A2\",\"stream\":\"in\"}},{\"from\":{\"flowlet\":\"A2\",\"stream\":\"out\"},\"to\":{\"flowlet\":\"A3\",\"stream\":\"in\"}}],\"streams\":{}}", StateChangeType.DEPLOYED);
    addPointToFlowStateTable(27, "demo", "UVW", null, "targetting", "{\"meta\":{\"name\":\"test\",\"email\":\"me@continuuity.com\",\"app\":\"personalization\"},\"flowlets\":[{\"name\":\"A1\",\"classname\":\"com.continuuity.flow.flowlet.api.Flowlet\",\"instances\":1,\"resource\":{\"cpu\":1,\"memory\":512,\"uplink\":1,\"downlink\":1},\"id\":0},{\"name\":\"A2\",\"classname\":\"com.continuuity.flow.flowlet.api.Flowlet\",\"instances\":1,\"resource\":{\"cpu\":1,\"memory\":512,\"uplink\":1,\"downlink\":1},\"id\":0},{\"name\":\"A3\",\"classname\":\"com.continuuity.flow.flowlet.api.Flowlet\",\"instances\":2,\"resource\":{\"cpu\":1,\"memory\":512,\"uplink\":1,\"downlink\":1},\"id\":0}],\"connections\":[{\"from\":{\"flowlet\":\"A1\",\"stream\":\"out\"},\"to\":{\"flowlet\":\"A2\",\"stream\":\"in\"}},{\"from\":{\"flowlet\":\"A2\",\"stream\":\"out\"},\"to\":{\"flowlet\":\"A3\",\"stream\":\"in\"}}],\"streams\":{}}", StateChangeType.DEPLOYED);
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
