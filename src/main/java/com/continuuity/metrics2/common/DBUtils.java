package com.continuuity.metrics2.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Utilities for Database operations.
 */
public class DBUtils {
  private static final Logger Log = LoggerFactory.getLogger(DBUtils.class);

  /**
   * Type
   */
  public enum DBType {
    HSQLDB,
    MYSQL,
    UNKNOWN;
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
    if(connectionUrl != null) {
      if(connectionUrl.contains("hsqldb")) {
        Class.forName("org.hsqldb.jdbcDriver");
        type = DBType.HSQLDB;
      } else if(connectionUrl.contains("mysql")) {
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
   * @return true if all tables created; false otherwise
   */
  public static boolean createMetricsTables(Connection connection, DBType type) {

    // Creates table only when it's of type HyperSQLDB.
    if(type != DBUtils.DBType.HSQLDB) {
      return true;
    }

    // DDL for creating metrics tables.
    String metricsTableCreateDDL = "CREATE TABLE metrics\n" +
      "   (account_id VARCHAR(64) NOT NULL, \n" +
      "    application_id VARCHAR(64) NOT NULL, \n" +
      "    flow_id VARCHAR(64) NOT NULL, \n" +
      "    flowlet_id VARCHAR(64) NOT NULL, \n" +
      "    instance_id INT DEFAULT 1, \n" +
      "    metric VARCHAR(64), \n" +
      "    value FLOAT,\n" +
      "    last_updt DATETIME,\n" +
      " PRIMARY KEY(account_id, application_id, flow_id, flowlet_id, " +
      "             instance_id, metric))";

    try {
      connection
        .prepareStatement("SET DATABASE DEFAULT RESULT MEMORY ROWS 1000")
        .execute();
      connection.prepareStatement(metricsTableCreateDDL).execute();
    } catch (SQLException e) {
      // SQL state for determining the duplicate table create exception
      // http://docs.oracle.com/javase/tutorial/jdbc/basics/sqlexception.html
      if(!e.getSQLState().equalsIgnoreCase("42504")) {
        Log.warn("Failed creating tables. Reason : {}", e.getMessage());
        return false;
      }
    }
    return true;
  }
}
