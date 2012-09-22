package com.continuuity.metrics2.collector.server.plugins;

import akka.dispatch.ExecutionContext;
import akka.dispatch.ExecutionContexts;
import akka.dispatch.Future;
import akka.dispatch.Futures;
import com.continuuity.common.builder.BuilderException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.db.DBConnectionPoolManager;
import com.continuuity.metrics2.collector.MetricRequest;
import com.continuuity.metrics2.collector.MetricResponse;
import com.continuuity.metrics2.common.DBUtils;
import com.google.common.collect.Maps;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import org.hsqldb.jdbc.pool.JDBCPooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.*;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Concrete implementation of {@link MetricsProcessor} for processing
 * the metrics received by MetricsCollectionServer of type
 * {@code MetricType.FlowSystem} and {@code MetricType.FlowSystem}.
 *
 * <p>
 *   Metrics are essentially written to a backend database that supports
 *   SQL.
 * </p>
 */
public final class FlowMetricsProcessor implements MetricsProcessor {
  private static final Logger Log
    = LoggerFactory.getLogger(FlowMetricsProcessor.class);

  /**
   * JDBC url.
   */
  private final String connectionUrl;

  /**
   * Type of DB being used.
   */
  private DBUtils.DBType type;

  /**
   * Executor service instance.
   */
  private final ExecutorService es = Executors.newFixedThreadPool(50);

  /**
   * Execution context under which the DB updates will happen.
   */
  private final ExecutionContext ec
    = ExecutionContexts.fromExecutorService(es);

  /**
   * Connection Pool Manager.
   */
  private DBConnectionPoolManager poolManager;

  /**
   * Number of points collected.
   */
  private Map<String, Integer> collectedPoints = Maps.newHashMap();

  /**
   * Allowed time series metrics.
   */
  private Map<String, Boolean> allowedTimeseriesMetrics = Maps.newHashMap();

  /**
   * Constructs and initializes a flow metric processor.
   *
   * @param configuration objects
   * @throws Exception in case of sql errors.
   */
  public FlowMetricsProcessor(CConfiguration configuration)
    throws Exception {
    // retrieve the connection url specified from configuation.
    this.connectionUrl
      = configuration.get(Constants.CFG_METRICS_CONNECTION_URL,
                          Constants.DEFAULT_METIRCS_CONNECTION_URL);

    String[] allowedMetrics
      = configuration.getStrings(
            Constants.CFG_METRICS_COLLECTION_ALLOWED_TIMESERIES_METRICS,
            Constants.DEFAULT_METRICS_COLLECTION_ALLOWED_TIMESERIES_METRICS
        );

    for(String metric : allowedMetrics) {
      allowedTimeseriesMetrics.put(metric, true);
    }

    // Load the appropriate driver.
    this.type = DBUtils.loadDriver(connectionUrl);

    // Creates a pooled data source.
    if(this.type == DBUtils.DBType.MYSQL) {
      MysqlConnectionPoolDataSource mysqlDataSource =
        new MysqlConnectionPoolDataSource();
      mysqlDataSource.setUrl(connectionUrl);
      poolManager = new DBConnectionPoolManager(mysqlDataSource, 40);
    } else if(this.type == DBUtils.DBType.HSQLDB) {
      JDBCPooledDataSource jdbcDataSource = new JDBCPooledDataSource();
      jdbcDataSource.setUrl(connectionUrl);
      poolManager = new DBConnectionPoolManager(jdbcDataSource, 40);
    }

    // Create any tables needed for initializing.
    DBUtils.createMetricsTables(getConnection(), this.type);

  }

  /**
   * @return a {@link java.sql.Connection} from the connection pool.
   * @throws SQLException thrown in case of any error.
   */
  private Connection getConnection() throws SQLException {
    if(poolManager != null) {
      return poolManager.getValidConnection();
    }
    return null;
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
      "   CAST('%s' AS VARCHAR(64))," +
      "   CAST(%d AS INT)," +
      "   CAST('%s' AS VARCHAR(64))" +
      "))" +
      "AS vals(" +
      "      account_id," +
      "      application_id," +
      "      flow_id," +
      "      run_id, " +
      "      flowlet_id," +
      "      instance_id," +
      "      metric" +
      ")" +
      "ON " +
      "      metrics.account_id = vals.account_id AND " +
      "      metrics.application_id = vals.application_id AND " +
      "      metrics.flow_id = vals.flow_id AND " +
      "      metrics.flowlet_id = vals.flowlet_id AND " +
      "      metrics.run_id = vals.run_id AND " +
      "      metrics.instance_id = vals.instance_id AND " +
      "      metrics.metric = vals.metric " +
      " WHEN MATCHED THEN UPDATE SET " +
      "      metrics.value = %f, metrics.last_updt = now() " +
      " WHEN NOT MATCHED THEN INSERT VALUES (" +
      "      '%s', '%s', '%s', '%s', '%s', %d, '%s', %f, now())";

    // Bind parameters in prepared statements can be used only
    // for queries. As this sql is not exactly we cannot use prepared
    // statement value injection.
    sql = String.format(sql,
                        elements.getAccountId(),
                        elements.getApplicationId(),
                        elements.getFlowId(),
                        elements.getRunId(),
                        elements.getFlowletId(),
                        elements.getInstanceId(),
                        elements.getMetric(),
                        request.getValue(),
                        elements.getAccountId(),
                        elements.getApplicationId(),
                        elements.getFlowId(),
                        elements.getRunId(),
                        elements.getFlowletId(),
                        elements.getInstanceId(),
                        elements.getMetric(),
                        request.getValue()
                        );

    Connection connection = null;
    PreparedStatement stmt = null;

    try {
      connection = getConnection();

      if(connection == null) {
        return false;
      }

      stmt = connection.prepareStatement(sql);
      stmt.executeUpdate();

      if(stmt != null) {
        stmt.close();
      }

      // If metric is not present then we don't attempt to
      // write the time series for that metric.
      if(! allowedTimeseriesMetrics.containsKey(elements.getMetric())) {
        return true;
      }

      sql =
        "INSERT INTO timeseries (" +
          "   account_id, " +
          "   application_id, " +
          "   flow_id, " +
          "   run_id, " +
          "   flowlet_id, " +
          "   instance_id, " +
          "   metric, " +
          "   timestamp, " +
          "   value " +
          ")" +
          " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ";
      stmt = connection.prepareStatement(sql);
      stmt.setString(1, elements.getAccountId());
      stmt.setString(2, elements.getApplicationId());
      stmt.setString(3, elements.getFlowId());
      stmt.setString(4, elements.getRunId());
      stmt.setString(5, elements.getFlowletId());
      stmt.setInt(6, elements.getInstanceId());
      stmt.setString(7, elements.getMetric());
      stmt.setLong(8, request.getTimestamp());
      stmt.setFloat(9, request.getValue());
      stmt.executeUpdate();

    } catch (SQLException e) {
      Log.warn("Failed writing metric to HSQLDB. Elements {},Reason : {}",
               elements.toString(), e.getMessage());
      return false;
    } finally {
      try {
        if(stmt != null) {
          stmt.close();
        }
        if(connection != null) {
          connection.close();
        }
      } catch(SQLException e) {
        Log.warn("Failed to close connection or statement. Reason : {}.",
                 e.getMessage());
      }
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
      "   run_id, " +
      "   flowlet_id, " +
      "   instance_id, " +
      "   metric, " +
      "   value, " +
      "   last_updt " +
      ")" +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, now()) " +
      "ON DUPLICATE KEY UPDATE value = ?, last_updt = now()";

    Connection connection = null;
    PreparedStatement stmt = null;
    try {
      connection = getConnection();
      connection.setAutoCommit(false);

      stmt = connection.prepareStatement(sql);
      stmt.setString(1, elements.getAccountId());
      stmt.setString(2, elements.getApplicationId());
      stmt.setString(3, elements.getFlowId());
      stmt.setString(4, elements.getRunId());
      stmt.setString(5, elements.getFlowletId());
      stmt.setInt(6, elements.getInstanceId());
      stmt.setString(7, elements.getMetric());
      stmt.setFloat(8, request.getValue());
      stmt.setFloat(9, request.getValue());
      stmt.executeUpdate();

      if(stmt != null) {
        stmt.close();
      }

      // We have collected metrics for 60 seconds at 1 second interval,
      // once we have that, we change to collecting the metric to 1 min
      // interval. This is done for 2 reasons. 1. That we immediately
      // get the reporting of data points, but once the points are filled
      // up to the actual resolution we want to collect data we change that
      // to final resolution. 2. In order to reduce the number of data
      // points stored in DB.
      if(collectedPoints.get(elements.getMetric()) > 60 &&
          request.getTimestamp() % 60 != 0) {
        return true;
      }

      sql =
        "INSERT INTO timeseries (" +
          "   account_id, " +
          "   application_id, " +
          "   flow_id, " +
          "   run_id, " +
          "   flowlet_id, " +
          "   instance_id, " +
          "   metric, " +
          "   timestamp, " +
          "   value " +
          ")" +
          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
          "ON DUPLICATE KEY UPDATE value = ?";
      stmt = connection.prepareStatement(sql);
      stmt.setString(1, elements.getAccountId());
      stmt.setString(2, elements.getApplicationId());
      stmt.setString(3, elements.getFlowId());
      stmt.setString(4, elements.getRunId());
      stmt.setString(5, elements.getFlowletId());
      stmt.setInt(6, elements.getInstanceId());
      stmt.setString(7, elements.getMetric());
      stmt.setLong(8, request.getTimestamp());
      stmt.setFloat(9, request.getValue());
      stmt.setFloat(10, request.getValue());
      stmt.executeUpdate();
    } catch (SQLException e) {
      Log.error("Failed writing metrics to MYSQLDB. Reason : {}",
                e.getMessage());
      return false;
    } finally {
      try {
        if(stmt != null) {
          stmt.close();
        }
        if(connection != null) {
          connection.commit();
          connection.close();
        }
      } catch (SQLException e) {
        Log.warn("Unable to close connection/statement. Reason : {}.",
                 e.getMessage());
      }
    }
    return true;
  }

  private boolean insertUpdateCounters(FlowMetricElements elements,
                                       MetricRequest request) {
    Log.debug("Active connections {}, InActive connections {}",
              poolManager.getActiveConnections(),
              poolManager.getInactiveConnections());
    if(type == DBUtils.DBType.HSQLDB) {
      return insertUpdateHSQLDB(elements, request);
    } else if(type == DBUtils.DBType.MYSQL) {
      return insertUpdateMYSQLDB(elements, request);
    }
    return false;
  }

  /**
   * Invoked when a {@link MetricRequest} type matches either the
   * <code>FlowSystem</code> or <code>FlowUser</code>.
   *
   * @param request that needs to be processed.
   * @return A future of writing the metric to DB with processing status.
   * @throws IOException
   */
  @Override
  public Future<MetricResponse.Status>  process(final MetricRequest request)
      throws IOException {
    // Future that returns an invalid status.
    final Future<MetricResponse.Status> invalidFutureResponse =
      Futures.future(new Callable<MetricResponse.Status>() {
        @Override
        public MetricResponse.Status call() throws Exception {
          return MetricResponse.Status.INVALID;
        }
      }, ec);

    // Break down the metric name into it's components.
    // If there are any issue with how it's constructed,
    // send a failure back and log a message on the server.
    try {
      final FlowMetricElements elements =
          new FlowMetricElements.Builder(request.getMetricName()).create();
      if(elements != null) {
        return Futures.future(new Callable<MetricResponse.Status>() {
          public MetricResponse.Status call() {
            if (insertUpdateCounters(elements, request)) {
              Log.debug("Successfully processed metric {}.", request.toString());
              return MetricResponse.Status.SUCCESS;
            }
            return MetricResponse.Status.FAILED;
          }
        }, ec);
      } else {
        Log.debug("Invalid flow metric elements for request {}",
                  request.toString());
      }
    } catch (BuilderException e) {
      Log.warn("Invalid flow metric received. Reason : {}.", e.getMessage());
    }
    return invalidFutureResponse;
  }

  /**
   * Closes the DB
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    Statement st = null;
    try {
      if(type == DBUtils.DBType.HSQLDB) {
        st = getConnection().createStatement();
        st.execute("SHUTDOWN");
        st.close();
      }
      poolManager.dispose();
    } catch (SQLException e) {
      Log.warn("Failed while shutting down. Reason : {}", e.getMessage());
    } finally {
      if(st != null) {
        try {
          st.close();
        } catch (SQLException e){
          Log.warn("Failed to close statement. Reason : {}", e.getMessage());
        }
      }
    }

    if(es != null) {
      es.shutdown();
    }
  }
}
