package com.continuuity.overlord.metrics.service;

import com.continuuity.overlord.metrics.stubs.FlowMetric;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

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

  public void initialization() throws SQLException {
    connection.prepareStatement(
      "CREATE TABLE flow_metrics (timestamp INTEGER, accountid VARCHAR, " +
        " app VARCHAR, flow VARCHAR, rid VARCHAR, version VARCHAR, flowlet VARCHAR, instance VARCHAR, metric VARCHAR, value INTEGER )"
    ).execute();
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
  public void stop(String s) {
    Log.info("Stopping flow monitoring ...");
    running = false;
  }

  @Override
  public boolean isStopped() {
    return ! running;
  }
}
