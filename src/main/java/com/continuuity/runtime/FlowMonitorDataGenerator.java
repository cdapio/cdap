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

    addPointToFlowStateTable(19, "demo", "XYZ", "targetting", "", StateChangeType.STARTING);
    addPointToFlowStateTable(20, "demo", "XYZ", "targetting", "", StateChangeType.STARTED);
    addPointToFlowStateTable(21, "demo", "XYZ", "targetting", "", StateChangeType.RUNNING);
    addPointToFlowStateTable(22, "demo", "XYZ", "targetting", "", StateChangeType.STOPPING);
    addPointToFlowStateTable(23, "demo", "XYZ", "targetting", "", StateChangeType.STOPPED);
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
