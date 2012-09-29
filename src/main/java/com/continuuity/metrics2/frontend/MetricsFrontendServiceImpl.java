package com.continuuity.metrics2.frontend;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.db.DBConnectionPoolManager;
import com.continuuity.metrics2.common.DBUtils;
import com.continuuity.metrics2.stubs.*;
import com.continuuity.metrics2.stubs.MetricsFrontendService;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.thrift.TException;
import org.hsqldb.jdbc.pool.JDBCPooledDataSource;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.*;
import java.util.List;
import java.util.Map;

/**
 * MetricsService provides a readonly service for metrics.
 * It's a implementation that reads from the SQL supported DB.
 */
public class MetricsFrontendServiceImpl
  implements MetricsFrontendService.Iface {
  private static final Logger Log = LoggerFactory.getLogger(
    MetricsFrontendServiceImpl.class
  );

  /**
   * Connection string to connect to database.
   */
  private String connectionUrl;

  /**
   * Type of Database we are configured with.
   */
  private DBUtils.DBType type;

  /**
   * DB Connection Pool manager.
   */
  private static DBConnectionPoolManager poolManager;

  public MetricsFrontendServiceImpl(CConfiguration configuration)
    throws ClassNotFoundException, SQLException {
    this.connectionUrl
      = configuration.get(Constants.CFG_METRICS_CONNECTION_URL,
                          Constants.DEFAULT_METIRCS_CONNECTION_URL);
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

    DBUtils.createMetricsTables(getConnection(), this.type);
  }

  /**
   * @return a {@link java.sql.Connection} based on the <code>connectionUrl</code>
   * @throws java.sql.SQLException thrown in case of any error.
   */
  private synchronized Connection getConnection() throws SQLException {
    if(poolManager != null) {
      return poolManager.getValidConnection();
    }
    return null;
  }

  /**
   * @throws IllegalArgumentException thrown if issue with arguments.
   */
  private void validateArguments(FlowArgument argument)
    throws MetricsServiceException {

    // Check if there are arguments, if there are none, then we cannot
    // proceed further.
    if(argument == null) {
      throw new MetricsServiceException(
        "Arguments specifying the flow has not been provided. Please specify " +
          "account, application, flow id"
      );
    }

    if(argument.getAccountId() == null || argument.getAccountId().isEmpty()) {
      throw new MetricsServiceException("Account ID has not been specified.");
    }

    if(argument.getApplicationId() == null ||
       argument.getApplicationId().isEmpty()) {
      throw new MetricsServiceException("Application ID has not been specified");
    }

    if(argument.getFlowId() == null ||
       argument.getFlowId().isEmpty()) {
      throw new MetricsServiceException("Flow ID has not been specified.");
    }
  }

  /**
   * Retrieves the counters as per the {@link CounterRequest} specification.
   *
   * @param request for counters.
   * @return list of {@link Counter}
   * @throws MetricsServiceException
   * @throws TException raised when thrift related issues.
   */
  @Override
  public List<Counter> getCounters(CounterRequest request)
    throws MetricsServiceException, TException {
    List<Counter> results = Lists.newArrayList();

    // Validate all the fields passed, if any problem return an exception
    // back to client.
    validateArguments(request.getArgument());

    // If metric name list is zero, then we return all the metrics.
    String sql = null;
    if(request.getName() == null || request.getName().size() == 0) {
      sql = "SELECT flowlet_id, metric, SUM(value) AS aggr_value FROM metrics " +
      "WHERE account_id = ? AND application_id = ? AND flow_id = ?" +
      "GROUP BY flowlet_id, metric";
    } else {
      // transform the metric names by adding single quotes around
      // each metric name as they are treated as metric.
      Iterable<String> iterator =
        Iterables.transform(request.getName(), new Function<String, String>() {
        @Override
        public String apply(@Nullable String input) {
          return "'" + input + "'";
        }
      });

      // Join each with comma (,) as seperator.
      String values = Joiner.on(",").join(iterator);

      sql = "SELECT flowlet_id, metric, SUM(value) AS aggr_value FROM " +
        "metrics WHERE account_id = ? AND application_id = ? AND flow_id = ? " +
        " AND metric in (" + values + ") GROUP BY flowlet_id, metric";
    }

    Connection connection = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      connection = getConnection();
      stmt = connection.prepareStatement(sql);
      stmt.setString(1, request.getArgument().getAccountId());
      stmt.setString(2, request.getArgument().getApplicationId());
      stmt.setString(3, request.getArgument().getFlowId());
      rs = stmt.executeQuery();
      while(rs.next()) {
        results.add(new Counter(
          rs.getString("flowlet_id"),
          rs.getString("metric"),
          rs.getFloat("aggr_value")
        ));
      }
    } catch (SQLException e) {
      Log.warn("Unable to retrieve counters. Reason : {}", e.getMessage());
    } finally {
      try {
        if(rs != null) {
          rs.close();
        }
        if(stmt != null) {
          stmt.close();
        }
        if(connection != null) {
          connection.close();
        }
      } catch(SQLException e) {
        Log.warn("Failed to close connection/statement/record. Reason : {}",
                 e.getMessage());
      }
    }

    return results;
  }

  private void validateTimeseriesRequest(TimeseriesRequest request)
    throws MetricsServiceException {

    if(! request.isSetArgument()) {
      throw new MetricsServiceException("Flow arguments should be specified.");
    }

    if(! request.isSetEndts()) {
      throw new MetricsServiceException("End time needs to be set");
    }

    if(! request.isSetMetrics()) {
      throw new MetricsServiceException("No metrics specified");
    }
  }

  /**
   * API to request time series data for a set of metrics.
   *
   * @param request
   */
  @Override
  public DataPoints getTimeSeries(TimeseriesRequest request)
    throws MetricsServiceException, TException {


    // Validate the timing request.
    validateTimeseriesRequest(request);

    MetricTimeseriesLevel level = MetricTimeseriesLevel.FLOW_LEVEL;
    if(request.isSetLevel()) {
      level = request.getLevel();
    }

    // transform the metric names by adding single quotes around
    // each metric name as they are treated as metric.
    Iterable<String> iterator =
      Iterables.transform(request.getMetrics(), new Function<String, String>() {
        @Override
        public String apply(@Nullable String input) {
          return "'" + input + "'";
        }
      });

    // Join each with comma (,) as seperator.
    String values = Joiner.on(",").join(iterator);

    Connection connection = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;

    long start = System.currentTimeMillis()/1000;;
    long end = start;

    DataPoints results = new DataPoints();

    try {

      // If start time is specified and end time is negative offset
      // from that start time, then we use that.
      if(request.isSetStartts() && request.getEndts() < 0) {
        start = request.getStartts() - request.getEndts() * 1000;
        end = request.getStartts();
      }

      // If endts is negative and the startts is not set, then we offset it
      // from the current time.
      if(! request.isSetStartts() && request.getEndts() < 0) {
        start = request.getStartts() - request.getEndts() * 1000;
      }

      // if startts is set and endts > 0 then it endts has to be greater than
      // startts.
      if(request.isSetStartts() && request.getEndts() > 0) {
        if(request.getEndts() < request.getEndts()) {
          throw new MetricsServiceException("End time is less than start time");
        }
        start = request.getStartts();
        end = request.getEndts();
      }

      // Get the connection for database.
      connection = getConnection();

      // Generates statement for retrieving metrics at run level.
      if(level == MetricTimeseriesLevel.RUNID_LEVEL) {
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT timestamp, metric, SUM(value) AS aggregate");
        sb.append(" ").append(" FROM timeseries");
        sb.append(" ").append("WHERE");
        sb.append(" ").append("account_id = ? AND");
        sb.append(" ").append("application_id = ? AND");
        sb.append(" ").append("flow_id = ? AND");
        sb.append(" ").append("run_id = ? AND");
        sb.append(" ").append("timestamp >= ? AND");
        sb.append(" ").append("timestamp < ? AND");
        sb.append(" ").append("metric IN ( ").append(values).append(" )") ;
        sb.append(" ").append("GROUP BY timestamp, metric");
        sb.append(" ").append("ORDER BY timestamp");

        // Connection
        stmt = connection.prepareStatement(sb.toString());
        stmt.setString(1, request.getArgument().getAccountId());
        stmt.setString(2, request.getArgument().getApplicationId());
        stmt.setString(3, request.getArgument().getFlowId());
        stmt.setString(4, request.getArgument().getRunId());
        stmt.setLong(5, start);
        stmt.setLong(6, end);
      } else if(level == MetricTimeseriesLevel.ACCOUNT_LEVEL) {
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT timestamp, metric, SUM(value) AS aggregate");
        sb.append(" ").append(" FROM timeseries");
        sb.append(" ").append("WHERE");
        sb.append(" ").append("account_id = ? AND");
        sb.append(" ").append("timestamp >= ? AND");
        sb.append(" ").append("timestamp < ? AND");
        sb.append(" ").append("metric IN ( ").append(values).append(" )") ;
        sb.append(" ").append("GROUP BY timestamp, metric");
        sb.append(" ").append("ORDER BY timestamp");
        stmt = connection.prepareStatement(sb.toString());
        stmt.setString(1, request.getArgument().getAccountId());
        stmt.setLong(2, start);
        stmt.setLong(3, end);
      } else if(level == MetricTimeseriesLevel.APPLICATION_LEVEL) {
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT timestamp, metric, SUM(value) AS aggregate");
        sb.append(" ").append(" FROM timeseries");
        sb.append(" ").append("WHERE");
        sb.append(" ").append("account_id = ? AND");
        sb.append(" ").append("application_id = ? AND");
        sb.append(" ").append("timestamp >= ? AND");
        sb.append(" ").append("timestamp < ? AND");
        sb.append(" ").append("metric IN ( ").append(values).append(" )") ;
        sb.append(" ").append("GROUP BY timestamp, metric");
        sb.append(" ").append("ORDER BY timestamp");
        stmt = connection.prepareStatement(sb.toString());
        stmt.setString(1, request.getArgument().getAccountId());
        stmt.setString(2, request.getArgument().getApplicationId());
        stmt.setLong(3, start);
        stmt.setLong(4, end);
      } else if(level == MetricTimeseriesLevel.FLOW_LEVEL) {
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT timestamp, metric, SUM(value) AS aggregate");
        sb.append(" ").append(" FROM timeseries");
        sb.append(" ").append("WHERE");
        sb.append(" ").append("account_id = ? AND");
        sb.append(" ").append("application_id = ? AND");
        sb.append(" ").append("flow_id = ? AND");
        sb.append(" ").append("timestamp >= ? AND");
        sb.append(" ").append("timestamp < ? AND");
        sb.append(" ").append("metric IN ( ").append(values).append(" )") ;
        sb.append(" ").append("GROUP BY timestamp, metric");
        sb.append(" ").append("ORDER BY timestamp");
        stmt = connection.prepareStatement(sb.toString());
        stmt.setString(1, request.getArgument().getAccountId());
        stmt.setString(2, request.getArgument().getApplicationId());
        stmt.setString(3, request.getArgument().getFlowId());
        stmt.setLong(4, start);
        stmt.setLong(5, end);
      } else if(level == MetricTimeseriesLevel.FLOWLET_LEVEL) {
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT timestamp, metric, SUM(value) AS aggregate");
        sb.append(" ").append(" FROM timeseries");
        sb.append(" ").append("WHERE");
        sb.append(" ").append("account_id = ? AND");
        sb.append(" ").append("application_id = ? AND");
        sb.append(" ").append("flow_id = ? AND");
        sb.append(" ").append("flowlet_id = ? AND");
        sb.append(" ").append("timestamp >= ? AND");
        sb.append(" ").append("timestamp < ? AND");
        sb.append(" ").append("metric IN ( ").append(values).append(" )") ;
        sb.append(" ").append("GROUP BY timestamp, metric");
        sb.append(" ").append("ORDER BY timestamp");
        stmt = connection.prepareStatement(sb.toString());
        stmt.setString(1, request.getArgument().getAccountId());
        stmt.setString(2, request.getArgument().getApplicationId());
        stmt.setString(3, request.getArgument().getFlowId());
        stmt.setString(4, request.getArgument().getFlowletId());
        stmt.setLong(5, start);
        stmt.setLong(6, end);
      }

      // Execute the query.
      rs = stmt.executeQuery();

      Map<String, List<DataPoint>> points = Maps.newHashMap();
      Map<String, Double> previousPoint = Maps.newHashMap();
      Map<String, Double> latest = Maps.newHashMap();

      // Iterate through the points.
      while(rs.next()) {
        String metric = rs.getString("metric");
        long ts = rs.getLong("timestamp");
        double value = rs.getFloat("aggregate");
        double newValue = value;

        // If summary is set then collect summary stats.
        if(request.isSetSummary() && request.isSetSummary()) {
          latest.put(metric, value);
        }

        // As the points are counters, we need to compute
        // them at time intervals.
        if(previousPoint.containsKey(metric)) {
          double prevValue = previousPoint.get(metric);
          newValue = value - prevValue;
          // Sometimes we might not receive data points from all
          // components and hence will not be aggregated, so we
          // need to exclude them.
          if(newValue > prevValue*2) {
            newValue = prevValue;
          }
        }

        previousPoint.put(metric, value);

        // Create a data point.
        DataPoint point = new DataPoint(ts, newValue);

        // If already added
        if(points.containsKey(metric)) {
          points.get(metric).add(point);
        } else {
          List<DataPoint> newPoints = Lists.newArrayList();
          newPoints.add(point);
          points.put(metric, newPoints);
        }
      }

      // Trim the first point.
      for(Map.Entry<String, List<DataPoint>> entry : points.entrySet()) {
        // Remove the first data point as that's an aggregate.
        entry.getValue().remove(0);
      }

      // Sets the points retrieved.
      results.setPoints(points);

      // If summary was setup, then we need add all the summary
      // data to response.
      if(request.isSetSummary() && request.isSetSummary()) {
        results.setLatest(latest);
      }
    } catch (SQLException e) {
      Log.warn("Failed retrieving data for request {}. Reason : {}",
               request.toString(), e.getMessage());
    } finally {
      try {
        if(rs != null) {
          rs.close();
        }
        if(stmt != null) {
          stmt.close();
        }
        if(connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
        Log.warn("Failed closing recordset/statement/connection. Reason : {}",
                 e.getMessage());
      }
    }
    return results;
  }

}
