package com.continuuity.metrics.service;

import com.continuuity.metrics.stubs.FlowEvent;
import com.continuuity.metrics.stubs.FlowMetric;
import com.continuuity.metrics.stubs.Metric;
import com.continuuity.metrics.stubs.MetricType;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import java.sql.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 *
 *
 */
class SQLFlowMonitorHandler implements FlowMonitorHandler {
  private static final Logger Log = LoggerFactory.getLogger(SQLFlowMonitorHandler.class);
  private final String url;
  private final Connection connection;
  private volatile boolean running;

  @Inject
  public SQLFlowMonitorHandler(@Named("Flow Monitor JDBC URL") final String url) throws SQLException {
    this.url = url;
    connection = DriverManager.getConnection(url, "sa", "");
    initialization();
    running = false;
  }

  public Connection getConnection() {
    return connection;
  }

  public void initialization()  {
    try {
      connection.prepareStatement(
        "CREATE TABLE flow_metrics (timestamp INTEGER, accountid VARCHAR, " +
          " app VARCHAR, flow VARCHAR, rid VARCHAR, version VARCHAR, flowlet VARCHAR, instance VARCHAR, metric VARCHAR, value INTEGER )"
      ).execute();
    } catch (SQLException e) {
      /** Ignore this for now - as this is for dual purpose */
    }
  }

  @Override
  public void add(FlowMetric metric) {
    String sql = "INSERT INTO flow_metrics (timestamp, accountid, app, flow, rid, version, flowlet, instance, " +
      " metric, value) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    try {
      PreparedStatement stmt = connection.prepareStatement(sql);
      stmt.setLong(1, metric.getTimestamp());
      stmt.setString(2, metric.getAccountId());
      stmt.setString(3, metric.getApplication());
      stmt.setString(4, metric.getFlow());
      stmt.setString(5, metric.getRid());
      stmt.setString(6, metric.getVersion());
      stmt.setString(7, metric.getFlowlet());
      stmt.setString(8, metric.getInstance());
      stmt.setString(9, metric.getMetric());
      stmt.setLong(10, metric.getValue());
      stmt.executeUpdate();
    } catch (SQLException e) {
      Log.error("Failed to write the metric to SQL DB (state : {}). Reason : {}", metric.toString(), e.getMessage());
    }
  }

  @Override
  public List<FlowEvent> getFlowHistory(String accountId, String app, String flow) {
    return null;
  }


  /**
   *
   * @param accountId
   * @param app
   * @param flow
   * @param rid
   * @return
   */
  @Override
  public List<Metric> getFlowMetric(String accountId, String app, String flow, String rid) {
    List<Metric> results = Lists.newArrayList();
    String sql = "SELECT flowlet, metric, rid, SUM(value) AS total FROM flow_metrics WHERE accountId = ? AND app = ? AND " +
      "flow = ? AND rid = ? GROUP by flowlet, metric, rid";
    try {
      PreparedStatement stmt = connection.prepareStatement(sql);
      stmt.setString(1, accountId);
      stmt.setString(2, app);
      stmt.setString(3, flow);
      stmt.setString(4, rid);
      ResultSet rs  = stmt.executeQuery();
      while(rs.next()) {
        Metric metric = new Metric();
        metric.setId(rs.getString("flowlet"));
        metric.setType(MetricType.FLOWLET);
        metric.setName(rs.getString("metric"));
        metric.setValue(rs.getLong("total"));
        results.add(metric);
      }
    } catch (SQLException e) {
      Log.warn("Unable to retrieve flow metrics. Application '{}', Flow '{}', Run ID '{}'",
        new Object[] {app, flow, rid});
    }
    return results;
  }

  @Override
  public void stop(String s) {
    Log.info("Stopping flow monitoring ...");
    running = false;
  }

  @Override
  public boolean isStopped() {
    return ! running;
  }
}
