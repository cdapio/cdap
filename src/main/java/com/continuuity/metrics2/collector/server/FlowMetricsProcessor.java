package com.continuuity.metrics2.collector.server;

import com.continuuity.common.builder.BuilderException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.metrics2.collector.MetricRequest;
import com.continuuity.metrics2.collector.MetricResponse;
import com.google.common.base.Preconditions;
import org.apache.mina.core.session.IoSession;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;

/**
 * Concrete implementation of {@link MetricsProcessor} for processing
 * the metrics received by {@link MetricsCollectionServer} of type
 * {@code MetricType.FlowSystem} and {@code MetricType.FlowSystem}.
 *
 * <p>
 *   Metrics are essentially written to a backend database that supports
 *   SQL.
 * </p>
 */
final class FlowMetricsProcessor implements MetricsProcessor {
  private static final Logger Log
    = LoggerFactory.getLogger(FlowMetricsProcessor.class);

  /**
   * Reference to the instance of configuration being passed.
   */
  private CConfiguration configuration;

  /**
   * JDBC url.
   */
  private final String jdbcUrl;

  /**
   * Type
   */
  private enum DBType {
    HSQLDB,
    MYSQL,
    UNKNOWN;
  }

  /**
   * Type of DB being used.
   */
  private DBType type;

  /**
   * Connection to Database.
   */
  private Connection connection = null;

  public FlowMetricsProcessor(CConfiguration configuration)
    throws Exception {

    this.configuration = configuration;
    this.jdbcUrl = configuration.get("overlord.jdbc.url",
                                     "jdbc:hsqldb:mem:aname?user=sa");

    if(jdbcUrl != null) {
      if(jdbcUrl.contains("hsqldb")) {
        Class.forName("org.hsqldb.jdbcDriver");
        type = DBType.HSQLDB;
        createTables();
      } else if(jdbcUrl.contains("mysql")) {
        Class.forName("com.mysql.jdbc.Driver");
        type = DBType.MYSQL;
      } else {
        type = DBType.UNKNOWN;
        throw new Exception("Unsupported driver specified in jdbc url : " +
                              jdbcUrl);
      }
    } else {
      type = DBType.UNKNOWN;
      throw new Exception("Invalid JDBC url specified.");
    }
  }

  /**
   * @return a {@link java.sql.Connection} based on the <code>jdbcUrl</code>
   * @throws SQLException thrown in case of any error.
   */
  private synchronized Connection getConnection() throws SQLException {
    if(connection == null) {
      connection = DriverManager.getConnection(jdbcUrl);
    }
    return connection;
  }


  /**
   * @return true if all tables created; false otherwise
   */
  public boolean createTables() {

    // Creates table only when it's of type HyperSQLDB.
    if(type != DBType.HSQLDB) {
      return true;
    }

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
      getConnection().prepareStatement(metricsTableCreateDDL).execute();
    } catch (SQLException e) {
      Log.warn("Failed creating tables. Reason : {}", e.getMessage());
      return false;
    }
    return true;
  }

  private boolean insertUpdateHSQLDB(FlowMetricElements elements,
                                     MetricRequest request) {
    String sql =
      "MERGE INTO metrics USING " +
      "(" +
      "VALUES (" +
      "   CAST('%s' AS VARCHAR(64))," +
      "   CAST('%s' AS VARCHAR(64))," +
      "   CAST('%s' AS VARCHAR(64))," +
      "   CAST('%s' AS VARCHAR(64))," +
      "   CAST(%d AS INT)," +
      "   CAST('%s' AS VARCHAR(64))" +
      "))" +
      "AS vals(" +
      "      account_id," +
      "      application_id," +
      "      flow_id," +
      "      flowlet_id," +
      "      instance_id," +
      "      metric" +
      ")" +
      "ON " +
      "      metrics.account_id = vals.account_id AND " +
      "      metrics.application_id = vals.application_id AND " +
      "      metrics.flow_id = vals.flow_id AND " +
      "      metrics.flowlet_id = vals.flowlet_id AND " +
      "      metrics.instance_id = vals.instance_id AND " +
      "      metrics.metric = vals.metric " +
      " WHEN MATCHED THEN UPDATE SET " +
      "      metrics.value = %f, metrics.last_updt = now() " +
      " WHEN NOT MATCHED THEN INSERT VALUES (" +
      "      '%s', '%s', '%s', '%s', %d, '%s', %f, now())";

    // Bind parameters in prepared statements can be used only
    // for queries. As this sql is not exactly we cannot use prepared
    // statement value injection.
    sql = String.format(sql,
                        elements.getAccountId(),
                        elements.getApplicationId(),
                        elements.getFlowId(),
                        elements.getFlowletId(),
                        elements.getInstanceId(),
                        elements.getMetric(),
                        request.getValue(),
                        elements.getAccountId(),
                        elements.getApplicationId(),
                        elements.getFlowId(),
                        elements.getFlowletId(),
                        elements.getInstanceId(),
                        elements.getMetric(),
                        request.getValue()
                        );

    try {
      Statement stmt = getConnection().createStatement();
      stmt.executeUpdate(sql);
    } catch (SQLException e) {
      Log.error("Failed writing metric to HSQLDB. Reason : {}",
                e.getMessage());
      return false;
    }
    return true;
  }

  private boolean insertUpdateMYSQLDB(FlowMetricElements elements,
                                      MetricRequest request) {
    String sql =
      "INSERT INTO metrics (" +
      "   account_id, " +
      "   application_id, " +
      "   flow_id, " +
      "   flowlet_id, " +
      "   instance_id, " +
      "   metric, " +
      "   value, " +
      "   last_updt " +
      ")" +
      "VALUES (?, ?, ?, ?, ?, ?, ?, now()) " +
      "ON DUPLICATE KEY UPDATE value = ?, last_updt = now()";

    try {
      PreparedStatement stmt = getConnection().prepareStatement(sql);
      stmt.setString(1, elements.getAccountId());
      stmt.setString(2, elements.getApplicationId());
      stmt.setString(3, elements.getFlowId());
      stmt.setString(4, elements.getFlowletId());
      stmt.setInt(5, elements.getInstanceId());
      stmt.setString(6, request.getMetricName());
      stmt.setFloat(7, request.getValue());
      stmt.setFloat(8, request.getValue());
      stmt.executeUpdate();
    } catch (SQLException e) {
      Log.error("Failed writing metrics to MYSQLDB. Reason : {}",
                e.getMessage());
      return false;
    }
    return true;
  }

  private boolean insertUpdate(FlowMetricElements elements,
                               MetricRequest request) {
    String sql = null;
    if(type == DBType.HSQLDB) {
      return insertUpdateHSQLDB(elements, request);
    } else if(type == DBType.MYSQL) {
      return insertUpdateMYSQLDB(elements, request);
    }
    return false;
  }

  @Override
  public void process(IoSession session, MetricRequest request)
      throws IOException {

    // Break down the metric name into it's components.
    // If there are any issue with how it's constructed,
    // send a failure back and log a message on the server.
    try {
      FlowMetricElements elements =
          new FlowMetricElements.Builder(request.getMetricName()).create();
      if(elements != null && insertUpdate(elements, request)) {
        session.write(new MetricResponse(MetricResponse.Status.SUCCESS));
      } else {
        sendFailure("Failed writing metric to DB", session);
      }
    } catch (BuilderException e) {
      sendFailure(e, session);
      return;
    }
  }

  private void sendFailure(Throwable t, IoSession session) {
    sendFailure(t.getMessage(), session);
  }

  private void sendFailure(String message, IoSession session) {
    Log.warn(message);
    session.write(new MetricResponse(MetricResponse.Status.FAILED));
  }

  private void sendSuccess(IoSession session) {
    session.write(new MetricResponse(MetricResponse.Status.SUCCESS));
  }
}
