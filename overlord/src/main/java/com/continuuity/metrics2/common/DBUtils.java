package com.continuuity.metrics2.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Utilities for Database operations.
 */
public class DBUtils {
  private static final Logger Log = LoggerFactory.getLogger(DBUtils.class);

  /**
   * Type.
   */
  public enum DBType {
    HSQLDB,
    MYSQL,
    UNKNOWN
  }

  /**
   * Loads appropriate DB driver based on the connection url specified.
   *
   * @param connectionUrl specifying how to connect to DB.
   * @return type of DB found and loaded.
   * @throws ClassNotFoundException
   */
  public static DBType loadDriver(String connectionUrl)
      throws ClassNotFoundException {
    DBType type;
    if (connectionUrl != null) {
      if (connectionUrl.contains("hsqldb")) {
        Class.forName("org.hsqldb.jdbcDriver");
        type = DBType.HSQLDB;
      } else if (connectionUrl.contains("mysql")) {
        Class.forName("com.mysql.jdbc.Driver");
        type = DBType.MYSQL;
      } else {
        type = DBType.UNKNOWN;
      }
    } else {
      type = DBType.UNKNOWN;
    }
    return type;
  }

  /**
   * Clears the metrics for a given program.
   *
   * @return true if successful; false otherwise.
   */
  public static boolean clearApplicationMetrics(Connection connection, String accountId, String applicationId) {

    try {
      PreparedStatement stmt = connection.prepareStatement(
                                  "DELETE FROM metrics WHERE (account_id=? OR account_id=?) AND application_id=?");
      try {
        stmt.setString(1, accountId);
        stmt.setString(2, "-");
        stmt.setString(3, applicationId);
        stmt.executeUpdate();
      } finally {
        stmt.close();
      }

      stmt = connection.prepareStatement(
                              "DELETE FROM timeseries WHERE (account_id=? OR account_id=?) AND application_id=?");
      try {
        stmt.setString(1, accountId);
        stmt.setString(2, "-");
        stmt.setString(3, applicationId);
        stmt.executeUpdate();
      } finally {
        stmt.close();
      }
      return true;
    } catch (SQLException e) {
      Log.error("Fail to remove metrics. ", e);
      return false;
    }
  }

  /**
   * Clears the tables for metrics.
   *
   * @param connection connection to DB
   * @param accountId who's data needs to be cleared.
   * @return true if successful; false otherwise.
   */
  public static boolean clearMetricsTables(Connection connection,
                                           String accountId) {
    PreparedStatement stmt = null;
    try {
      String sql = "DELETE FROM metrics WHERE account_id = ? OR account_id = ?";
      stmt = connection.prepareStatement(sql);
      stmt.setString(1, accountId);
      stmt.setString(2, "-");
      stmt.execute();
      if (stmt != null) {
        stmt.close();
        stmt = null;
      }
      sql = "DELETE FROM timeseries WHERE account_id = ? OR account_id = ?";
      stmt = connection.prepareStatement(sql);
      stmt.setString(1, accountId);
      stmt.setString(2, "-");
      stmt.execute();
      if (stmt != null) {
        stmt.close();
        stmt = null;
      }
      return true;
    } catch (SQLException e) {
      Log.warn("Failed clearing tables. Reason : {}", e.getMessage());
      return false;
    } finally {
      try {
        if (stmt != null) {
          stmt.close();
        }
        if (connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
        Log.warn("Failed closing connection/statement. Reason : {}",
                 e.getMessage());
      }
    }
  }

  /**
   * @return true if all tables created; false otherwise
   */
  public static boolean createMetricsTables(Connection connection, DBType type) {

    // Creates table only when it's of type HyperSQLDB.
    if (type != DBUtils.DBType.HSQLDB) {
      return true;
    }

    // DDL for creating metrics tables.
    String metricsTableCreateDDL = "CREATE CACHED TABLE metrics\n" +
      "   (account_id VARCHAR(64) NOT NULL, \n" +
      "    application_id VARCHAR(64) NOT NULL, \n" +
      "    flow_id VARCHAR(64) NOT NULL, \n" +
      "    run_id VARCHAR(64) NOT NULL, \n" +
      "    flowlet_id VARCHAR(64) NOT NULL, \n" +
      "    instance_id INT DEFAULT 1, \n" +
      "    metric VARCHAR(256), \n" +
      "    value FLOAT,\n" +
      "    last_updt DATETIME,\n" +
      " PRIMARY KEY(account_id, application_id, flow_id, run_id, flowlet_id," +
      "             instance_id, metric))";

    String metricsTimeseriesCreateTableDDL = "CREATE CACHED TABLE timeseries\n" +
      "   (account_id VARCHAR(64) NOT NULL, \n" +
      "    application_id VARCHAR(64) NOT NULL, \n" +
      "    flow_id VARCHAR(64) NOT NULL, \n" +
      "    run_id VARCHAR(64) NOT NULL, \n" +
      "    flowlet_id VARCHAR(64) NOT NULL, \n" +
      "    instance_id INT DEFAULT 1, \n" +
      "    metric VARCHAR(256), \n" +
      "    timestamp BIGINT,\n" +
      "    value FLOAT,\n" +
      " PRIMARY KEY(account_id, application_id, flow_id, run_id, flowlet_id," +
      "             instance_id, metric, timestamp))";

    PreparedStatement stmt = null;
    try {
      stmt = connection.prepareStatement(metricsTableCreateDDL);
      stmt.execute();

      if (stmt != null) {
        stmt.close();
        stmt = null;
      }

      // set database to default memory rows.
      stmt = connection.prepareStatement("SET DATABASE DEFAULT " +
                                           "RESULT MEMORY ROWS 1000");
      stmt.execute();

      if (stmt != null) {
        stmt.close();
        stmt = null;
      }

      stmt = connection.prepareStatement(metricsTimeseriesCreateTableDDL);
      stmt.execute();

      if (stmt != null){
        stmt.close();
        stmt = null;
      }
    } catch (SQLException e) {
      // SQL state for determining the duplicate table create exception
      // http://docs.oracle.com/javase/tutorial/jdbc/basics/sqlexception.html
      if (!e.getSQLState().equalsIgnoreCase("42504")) {
        Log.warn("Failed creating tables. Reason : {}", e.getMessage());
        return false;
      }
    } finally {
      try {
        if (stmt != null) {
          stmt.close();
        }
        if (connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
        Log.warn("Failed closing connection/statement. Reason : {}",
                 e.getMessage());
      }
    }
    return true;
  }
}
