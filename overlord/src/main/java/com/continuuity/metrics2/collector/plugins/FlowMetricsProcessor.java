package com.continuuity.metrics2.collector.plugins;

import akka.dispatch.ExecutionContext;
import akka.dispatch.ExecutionContexts;
import akka.dispatch.Future;
import akka.dispatch.Futures;
import com.continuuity.common.builder.BuilderException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.db.DBConnectionPoolManager;
import com.continuuity.common.metrics.MetricRequest;
import com.continuuity.common.metrics.MetricResponse;
import com.continuuity.metrics2.common.DBUtils;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import org.hsqldb.jdbc.pool.JDBCPooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
   * Allowed time series metrics.
   */
  private Map<String, Boolean> allowedTimeseriesMetrics = Maps.newHashMap();

  /**
   * TimeseriesCleaner is responsible for making sure the timeseries
   * data older than certain time is deleted.
   */
  private static final class TimeseriesCleanser
      extends AbstractScheduledService {
    private final FlowMetricsProcessor processor;
    private final long metricsTimeToLiveInSeconds;
    private final long cleanupPeriodInSeconds;

    public TimeseriesCleanser(FlowMetricsProcessor processor,
        long metricsTimeToLiveInSeconds, long cleanupPeriodInSeconds) {
      this.processor = processor;
      this.metricsTimeToLiveInSeconds = metricsTimeToLiveInSeconds;
      this.cleanupPeriodInSeconds = cleanupPeriodInSeconds;
    }

    @Override
    protected void runOneIteration() throws Exception {
      Connection connection = null;
      PreparedStatement stmt = null;
      StringBuffer sb = new StringBuffer();
      sb.append("DELETE FROM timeseries")
        .append(" ").append("WHERE timestamp < ?");
      try {
        connection = processor.getConnection();
        stmt = connection.prepareStatement(sb.toString());
        long oldestStartTime = ((System.currentTimeMillis() / 1000)
          - metricsTimeToLiveInSeconds);
        stmt.setLong(1, oldestStartTime);
        stmt.executeUpdate();
        Log.trace("Cleaning up timeseries DB older than timeperiod {}",
          oldestStartTime);
      } catch (SQLException e) {
        Log.warn("Failed to older timeseries data. Reason : {}",
          e.getMessage());
      } finally {
        try {
          if (connection != null) {
            connection.close();
          }
          if (stmt != null) {
            stmt.close();
          }
        } catch (SQLException e) {
          Log.warn("Failed to close connection/stmt. Reason : {}.",
            e.getMessage());
        }
      }
    }

    @Override
    protected Scheduler scheduler() {
      return Scheduler.newFixedRateSchedule(
          cleanupPeriodInSeconds, cleanupPeriodInSeconds, TimeUnit.SECONDS);
    }
  }

  /**
   * Instance of timeseries cleaner.
   */
  private static TimeseriesCleanser timeseriesCleanser = null;

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

    for (String metric : allowedMetrics) {
      allowedTimeseriesMetrics.put(metric, true);
    }

    // Load the appropriate driver.
    this.type = DBUtils.loadDriver(connectionUrl);

    // Creates a pooled data source.
    if (this.type == DBUtils.DBType.MYSQL) {
      MysqlConnectionPoolDataSource mysqlDataSource =
        new MysqlConnectionPoolDataSource();
      mysqlDataSource.setUrl(connectionUrl);
      poolManager = new DBConnectionPoolManager(mysqlDataSource, 100);
    } else if (this.type == DBUtils.DBType.HSQLDB) {
      JDBCPooledDataSource jdbcDataSource = new JDBCPooledDataSource();
      jdbcDataSource.setUrl(connectionUrl);
      jdbcDataSource.setProperties(getHsqlProperties(configuration));
      poolManager = new DBConnectionPoolManager(jdbcDataSource, 100);
    }

    // Starting the timeseries cleaners.
    if (timeseriesCleanser == null) {
      timeseriesCleanser = new TimeseriesCleanser(this,
          configuration.getLong(Constants.CFG_METRICS_CLEANUP_TIME_TO_LIVE,
              Constants.DEFAULT_METRICS_CLEANUP_TIME_TO_LIVE),
          configuration.getLong(Constants.CFG_METRICS_CLEANUP_PERIOD,
              Constants.DEFAULT_METRICS_CLEANUP_PERIOD));
      Log.debug("Starting timeseries db cleaner.");
      timeseriesCleanser.start();
    }

    // Create any tables needed for initializing.
    DBUtils.createMetricsTables(getConnection(), this.type);

  }

  private Properties getHsqlProperties(CConfiguration configuration) {
    Properties hsqlProperties = new Properties();
    // Assume 1K rows and 512MB cache size
    hsqlProperties.setProperty("hsqldb.cache_rows",
                               "" + configuration.getLong(Constants.CFG_DATA_HSQLDB_CACHE_ROWS,
                                                          Constants.DEFAULT_DATA_HSQLDB_CACHE_ROWS));
    hsqlProperties.setProperty("hsqldb.cache_size",
                               "" + configuration.getLong(Constants.CFG_DATA_HSQLDB_CACHE_SIZE,
                                                          Constants.DEFAULT_DATA_HSQLDB_CACHE_SIZE));
    // Disable logging
    hsqlProperties.setProperty("hsqldb.log_data", "false");
    return hsqlProperties;
  }

  /**
   * @return a {@link java.sql.Connection} from the connection pool.
   * @throws SQLException thrown in case of any error.
   */
  private Connection getConnection() throws SQLException {
    if (poolManager != null) {
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
      "   CAST('%s' AS VARCHAR(256))" +
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
      " metrics.value = %f, metrics.last_updt = now() " +
      " WHEN NOT MATCHED THEN INSERT VALUES (" +
      " '%s', '%s', '%s', '%s', '%s', %d, '%s', %f, now())";

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

      if (connection == null) {
        return false;
      }

      stmt = connection.prepareStatement(sql);
      stmt.executeUpdate();

      if (stmt != null) {
        stmt.close();
      }

      // If metric is not present then we don't attempt to
      // write the time series for that metric.
      // If metric has same value, then we don't add the point.
      if (!allowedTimeseriesMetrics.containsKey(elements.getMetric())
        && !elements.getMetric().contains(".stream.out")
        && !elements.getMetric().contains(".stream.in")
        && !elements.getMetric().contains("queue//")
        && !elements.getMetric().contains("stream//")
        && !elements.getMetric().contains("dataset.")) {
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
        if (stmt != null) {
          stmt.close();
        }
        if (connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
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

      if (stmt != null) {
        stmt.close();
      }

      // If metric is not present then we don't attempt to
      // write the time series for that metric.
      // If metric has same value, then we don't add the point.
      if (!allowedTimeseriesMetrics.containsKey(elements.getMetric())
        && !elements.getMetric().contains(".stream.out")
        && !elements.getMetric().contains(".stream.in")
        && !elements.getMetric().contains("queue//")
        && !elements.getMetric().contains("stream//")
        && !elements.getMetric().contains("dataset.")) {
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
        if (stmt != null) {
          stmt.close();
        }
        if (connection != null) {
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
    if (type == DBUtils.DBType.HSQLDB) {
      return insertUpdateHSQLDB(elements, request);
    } else if (type == DBUtils.DBType.MYSQL) {
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
  public ListenableFuture<MetricResponse.Status> process(final MetricRequest request)
      throws IOException {
    // Future that returns an invalid status.

    SettableFuture<MetricResponse.Status> response = SettableFuture.create();

    // Break down the metric name into it's components.
    // If there are any issue with how it's constructed,
    // send a failure back and log a message on the server.
    Log.trace("Received flow metric {}", request.toString());
    try {
      final FlowMetricElements elements =
          new FlowMetricElements.Builder(request.getMetricName()).create();
      if (elements != null) {
        if (insertUpdateCounters(elements, request)) {
          response.set(MetricResponse.Status.SUCCESS);
        } else {
          response.set(MetricResponse.Status.FAILED);
        }
      } else {
        Log.warn("Invalid flow metric elements for request {}",
                  request.toString());
      }
    } catch (BuilderException e) {
      Log.warn("Invalid flow metric received. Reason : {}.", e.getMessage());
    }
    response.set(MetricResponse.Status.FAILED);
    return response;
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
      if (type == DBUtils.DBType.HSQLDB) {
        st = getConnection().createStatement();
        st.execute("SHUTDOWN");
        st.close();
      }
      poolManager.dispose();
    } catch (SQLException e) {
      Log.warn("Failed while shutting down. Reason : {}", e.getMessage());
    } finally {
      if (st != null) {
        try {
          st.close();
        } catch (SQLException e){
          Log.warn("Failed to close statement. Reason : {}", e.getMessage());
        }
      }
    }

    if (es != null) {
      es.shutdown();
    }
  }
}
