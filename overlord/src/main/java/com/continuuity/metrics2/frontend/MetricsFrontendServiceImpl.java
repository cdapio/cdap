package com.continuuity.metrics2.frontend;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.db.DBConnectionPoolManager;
import com.continuuity.common.logging.LogCollector;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.metrics2.common.DBUtils;
import com.continuuity.metrics2.temporaldb.DataPoint;
import com.continuuity.metrics2.temporaldb.Timeseries;
import com.continuuity.metrics2.thrift.Counter;
import com.continuuity.metrics2.thrift.CounterRequest;
import com.continuuity.metrics2.thrift.FlowArgument;
import com.continuuity.metrics2.thrift.MetricTimeseriesLevel;
import com.continuuity.metrics2.thrift.MetricsFrontendService;
import com.continuuity.metrics2.thrift.MetricsServiceException;
import com.continuuity.metrics2.thrift.Point;
import com.continuuity.metrics2.thrift.Points;
import com.continuuity.metrics2.thrift.TimeseriesRequest;
import com.continuuity.weave.common.Threads;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.hsqldb.jdbc.pool.JDBCPooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * MetricsService provides a readonly service for metrics.
 * It's a implementation that reads from the SQL supported DB.
 */
public class MetricsFrontendServiceImpl
  implements MetricsFrontendService.Iface {

  private static final Logger LOG = LoggerFactory.getLogger(
    MetricsFrontendServiceImpl.class
  );

  private static short SKIP_POINTS = 10;

  private static int MAX_THREAD_POOL_SIZE = 50;

  /**
   * Connection string to connect to database.
   */
  private String connectionUrl;

  /**
   * Type of Database we are configured with.
   */
  private DBUtils.DBType type;

  private LogCollector collector;

  // Thread pool of size max MAX_THREAD_POOL_SIZE.
  // 60 seconds wait time before killing idle threads.
  // Keep no idle threads more than 60 seconds.
  // If max thread pool size reached, reject the new coming
  private final ExecutorService executor =
    new ThreadPoolExecutor(0, MAX_THREAD_POOL_SIZE,
                           60L, TimeUnit.SECONDS,
                           new SynchronousQueue<Runnable>(),
                           Threads.createDaemonThreadFactory("metrics-service-%d"),
                           new ThreadPoolExecutor.DiscardPolicy());


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
      poolManager = new DBConnectionPoolManager(mysqlDataSource, 1000);
    } else if(this.type == DBUtils.DBType.HSQLDB) {
      JDBCPooledDataSource jdbcDataSource = new JDBCPooledDataSource();
      jdbcDataSource.setUrl(connectionUrl);
      poolManager = new DBConnectionPoolManager(jdbcDataSource, 1000);
    }
    DBUtils.createMetricsTables(getConnection(), this.type);
    // It seems like not a good idea to pass hadoop config that way.
    // But using log collector here is bad anyways: overlord should not use logCollector
    // as a library, but rather should talk to it thru remote API.
    // This is going to be extracted anyways
    collector = new LogCollector(configuration, new Configuration());
  }

  private ExecutorService createExecutor() {
    // Thread pool of size max TX_EXECUTOR_POOL_SIZE.
    // 60 seconds wait time before killing idle threads.
    // Keep no idle threads more than 60 seconds.
    // If max thread pool size reached, reject the new coming
    return new ThreadPoolExecutor(0, MAX_THREAD_POOL_SIZE,
                                  60L, TimeUnit.SECONDS,
                                  new SynchronousQueue<Runnable>(),
                                  Threads.createDaemonThreadFactory("metrics-service-%d"),
                                  new ThreadPoolExecutor.DiscardPolicy());
  }

  /**
   * @return a {@link java.sql.Connection} based on the <code>connectionUrl</code>
   * @throws java.sql.SQLException thrown in case of any error.
   */
  private Connection getConnection() throws SQLException {
    if(poolManager != null) {
      return poolManager.getValidConnection();
    }
    return null;
  }

  @Override
  public void clear(String accountId, String applicationId) throws MetricsServiceException, TException {
    try {
      if (!DBUtils.clearApplicationMetrics(getConnection(), accountId, applicationId)) {
        throw new MetricsServiceException("Fail to reset metrics for application " +
                                            applicationId + " for account " + accountId);
      }

    } catch (SQLException e) {
      throw new MetricsServiceException(e.getMessage());
    }
  }

  /**
   * Resets the metrics for a given account.
   *
   * @param accountId for which the metrics needs to be set.
   * @throws MetricsServiceException thrown when there is issue with reseting
   * metrics.
   * @throws TException for Thrift level issues
   */
  @Override
  public void reset(String accountId) throws MetricsServiceException, TException {
    try {
      if(! DBUtils.clearMetricsTables(getConnection(), accountId)) {
        throw new MetricsServiceException("Failed to reset metrics for " +
                                            "account " + accountId);
      }
    } catch (SQLException e) {
      throw new MetricsServiceException(e.getMessage());
    }
  }

  @Override
  public List<String> getLog(final String accountId, final String applicationId,
                             final String flowId, int size)
    throws MetricsServiceException, TException {

    String logTag = String.format("%s:%s:%s", accountId, applicationId, flowId);

    if(size < 0) {
      size = 10 * 1024;
    }

    List<String> lines = null;
    try {
      lines = collector.tail(logTag, size);
      return lines;
    } catch (IOException e) {
      LOG.warn("Failed to tail log file. Tag {}. Reason : {}",
               logTag, e.getMessage());
      throw new MetricsServiceException(e.getMessage());
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

      // If run id is passed, then use it.
      String runIdInclusion = null;
      if(request.getArgument() != null &&
        request.getArgument().isSetRunId()) {
        runIdInclusion = String.format("run_id = '%s'",
          request.getArgument().getRunId());
      }

      // If metric name list is zero, then we return all the metrics.
      StringBuffer sql = new StringBuffer();
      if(request.getName() == null || request.getName().size() == 0) {
        sql.append("SELECT flowlet_id, metric, SUM(value) AS aggr_value");
        sql.append(" ");
        sql.append("FROM metrics WHERE account_id = ? AND application_id = ?");
        sql.append(" ");
        sql.append("AND flow_id = ?");
        sql.append(" ");
        if(runIdInclusion != null) {
          sql.append("AND").append(" ").append(runIdInclusion).append(" ");
        }
        sql.append("GROUP BY flowlet_id, metric");
      } else {
        // transform the metric names by adding single quotes around
        // each metric name as they are treated as metric.
        Iterable<String> iterator =
          Iterables.transform(request.getName(), new Function<String, String>() {
            @Override
            public String apply(String input) {
              return "'" + input + "'";
            }
          });

        // Join each with comma (,) as seperator.
        String values = Joiner.on(",").join(iterator);
        sql.append("SELECT flowlet_id, metric, SUM(value) AS aggr_value");
        sql.append(" ");
        sql.append("FROM metrics WHERE account_id = ? AND application_id = ?");
        sql.append(" ");
        sql.append("AND flow_id = ?");
        sql.append("AND");
        if(runIdInclusion != null) {
          sql.append(" ").append(runIdInclusion).append(" AND");
        }
        sql.append(" ").append("metric in (")
          .append(values).append(")").append(" ");
        sql.append("GROUP BY flowlet_id, metric");
      }

      Connection connection = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      try {
        connection = getConnection();
        stmt = connection.prepareStatement(sql.toString());
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
        LOG.warn("Unable to retrieve counters. Reason : {}", e.getMessage());
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
          LOG.warn("Failed to close connection/statement/record. Reason : " +
                     "{}", e.getMessage());
        }
      }

      return results;
  }

  /**
   * API to request time series data for a set of metrics.
   *
   * @param request
   */
  @Override
  public Points getTimeSeries(TimeseriesRequest request)
    throws MetricsServiceException, TException {
    List<Future<ImmutablePair<String, List<DataPoint>>>>
      dataPointsFuture = Lists.newArrayList();
    Timeseries timeseries = new Timeseries();

    long start = System.currentTimeMillis()/1000;
    long end = start - 1; // Skip few current datapoints, as they might be
    // being populated.

    // Validate the timing request.
    validateTimeseriesRequest(request);

    // If start time is specified and end time is negative offset
    // from that start time, then we use that.
    if(request.isSetStartts() && request.getStartts() < 0) {
      start = start + request.getStartts() + SKIP_POINTS;
    }

    if(request.isSetStartts() && request.isSetEndts()) {
      start = request.getStartts();
      end = request.getEndts();
    }

    // Preprocess the metrics list.
    List<String> preprocessedMetrics = Lists.newArrayList();
    for(String metric : request.getMetrics()) {
      if("busyness".equals(metric)) {
        preprocessedMetrics.add("tuples.read.count");
        preprocessedMetrics.add("tuples.attempt.read.count");
      } else {
        preprocessedMetrics.add(metric);
      }
    }

    // Iterate through the metric list to be retrieved and request them
    // to be fetched in parallel.
    for(String metric : preprocessedMetrics) {
      Callable<ImmutablePair<String, List<DataPoint>>> worker =
        new RetrieveDataPointCallable(metric, start, end, request);
      Future<ImmutablePair<String, List<DataPoint>>> submit = executor.submit(worker);
      dataPointsFuture.add(submit);
    }

    // Now, join on all dataPodints retrieved from future.
    long numPoints = Math.min(1800, end - start);
    Map<String, List<DataPoint>> dataPoints = Maps.newHashMap();
    for(Future<ImmutablePair<String, List<DataPoint>>> future : dataPointsFuture) {
      try {
        ImmutablePair<String, List<DataPoint>> dataPoint = future.get();
        dataPoints.put(dataPoint.getFirst(), dataPoint.getSecond());
      } catch (InterruptedException e) {
        LOG.info("Timeseries retrieval has been interrupted. Reason : {}",
                 e.getMessage());
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        LOG.warn("There was error getting results of a future. Reason : {}",
                 e.getMessage());
      }
    }

    Map<String, List<Point>> results = Maps.newHashMap();

    // Iterate through the list of metric requested and
    for(String metric : request.getMetrics()) {
      // If the metric to be retrieved is busyness, it's a composite metric
      // and hence we retrieve the tuple.read.count and tuples.proc.count
      // and divide one by the other. This is done on the rate.
      if(metric.equals("busyness")) {
        List<DataPoint> processed = dataPoints.get("tuples.read.count");
        List<DataPoint> read = dataPoints.get("tuples.attempt.read.count");
        if(read == null || processed == null) {
          List<DataPoint> n = null;
          results.put(metric, convertDataPointToPoint(n));
        } else {
          ImmutableList<DataPoint> busyness = timeseries.div(
            timeseries.rate(ImmutableList.copyOf(processed)),
            timeseries.rate(ImmutableList.copyOf(read)),
            new Function<Double, Double>() {
              @Override
              public Double apply(Double value) {
                if(value.doubleValue() > 1) {
                  value = new Double(1);
                }
                return value * 100;
              }
            }
          );
          ImmutableList<DataPoint> filledBusyness =
            timeseries.fill(busyness,"busyness",start, end, numPoints, 1);
          results.put(metric, convertDataPointToPoint(filledBusyness));
        }
      } else {
        ImmutableList<DataPoint> r =
          timeseries.rate(dataPoints.get(metric));
        ImmutableList<DataPoint> filledr =
          timeseries.fill(r, metric, start, end, numPoints, 1);
        results.put(metric, convertDataPointToPoint(filledr));
      }
    }

//    StringBuffer sb = new StringBuffer();
//    for(Map.Entry<String, List<Point>> entry : results.entrySet()) {
//      sb.append("Metric :").append(entry.getKey()).append("[");
//      for(Point point : entry.getValue()) {
//        sb.append(point.getValue()).append(",");
//      }
//      sb.append("]").append("\n");
//    }
//    System.out.println(sb.toString());
    Points points = new Points();
    points.setPoints(results);
    return points;
  }

  /**
   * Converts List<DataPoint> to List<Point>. This is essentially done
   * to return values through thrift to frontend.
   *
   * @param points specifies a list of datapoints to be transformed to list of
   *               point.
   * @return List<Point>
   */
  List<Point> convertDataPointToPoint(List<DataPoint> points) {
    List<Point> p = Lists.newArrayList();
    if(points == null || points.size() < 1) {
      return p;
    }
    short count = SKIP_POINTS;
    for(DataPoint point : points) {
      if(points.size() > SKIP_POINTS && count > 0) {
        count--;
        continue;
      }
      Point p1 = new Point();
      p1.setTimestamp(point.getTimestamp());
      p1.setValue(point.getValue());
      p.add(p1);
    }
    //Collections.reverse(p);
    return p;
  }

  /**
   * Callable that's responsible for retrieving the metric requested in
   * parallel from database.
   */
  private class RetrieveDataPointCallable
    implements Callable<ImmutablePair<String, List<DataPoint>>> {
    final String metric;
    final long start;
    final long end;
    final TimeseriesRequest request;

    public RetrieveDataPointCallable(String metric, long start, long end,
                                     TimeseriesRequest request) {
      this.metric = metric;
      this.start = start;
      this.end = end;
      this.request = request;
    }
    @Override
    public ImmutablePair<String, List<DataPoint>> call() throws Exception {
      long id = System.nanoTime();
      MetricTimeseriesLevel level = MetricTimeseriesLevel.FLOW_LEVEL;
      if(request.isSetLevel()) {
        level = request.getLevel();
      }
      List<DataPoint> points =
        getDataPoint(metric, level, start, end, request.getArgument());
      return new ImmutablePair<String, List<DataPoint>>(metric, points);
    }
  }

  /**
   * For a given metric returns a list of datapoint.
   *
   * @param metric name of metric.
   * @param level  level at which the metrics needs to be retrieved.
   * @param start  start timestamp
   * @param end    end timestamp
   * @param argument of a flow.
   * @return List<DataPoint>
   */
  List<DataPoint> getDataPoint(String metric, MetricTimeseriesLevel level,
                               long start, long end, FlowArgument argument) {
    Connection connection = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    List<DataPoint> results = new ArrayList<DataPoint>();

    try {
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
        sb.append(" ").append("metric = ?");
        sb.append(" ").append("GROUP BY timestamp, metric");
        sb.append(" ").append("ORDER BY timestamp");

        // Connection
        stmt = connection.prepareStatement(sb.toString());
        stmt.setString(1, argument.getAccountId());
        stmt.setString(2, argument.getApplicationId());
        stmt.setString(3, argument.getFlowId());
        stmt.setString(4, argument.getRunId());
        stmt.setLong(5, start);
        stmt.setLong(6, end);
        stmt.setString(7, metric);
        LOG.trace("Timeseries query {}", stmt.toString());
      } else if(level == MetricTimeseriesLevel.ACCOUNT_LEVEL) {
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT timestamp, metric, SUM(value) AS aggregate");
        sb.append(" ").append(" FROM timeseries");
        sb.append(" ").append("WHERE");
        sb.append(" ").append("account_id = ? AND");
        sb.append(" ").append("timestamp >= ? AND");
        sb.append(" ").append("timestamp < ? AND");
        sb.append(" ").append("metric = ?") ;
        sb.append(" ").append("GROUP BY timestamp, metric");
        sb.append(" ").append("ORDER BY timestamp");
        stmt = connection.prepareStatement(sb.toString());
        stmt.setString(1, argument.getAccountId());
        stmt.setLong(2, start);
        stmt.setLong(3, end);
        stmt.setString(4, metric);
        LOG.trace("Timeseries query {}", stmt.toString());
      } else if(level == MetricTimeseriesLevel.APPLICATION_LEVEL) {
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT timestamp, metric, SUM(value) AS aggregate");
        sb.append(" ").append(" FROM timeseries");
        sb.append(" ").append("WHERE");
        sb.append(" ").append("account_id = ? AND");
        sb.append(" ").append("application_id = ? AND");
        sb.append(" ").append("timestamp >= ? AND");
        sb.append(" ").append("timestamp < ? AND");
        sb.append(" ").append("metric = ?") ;
        sb.append(" ").append("GROUP BY timestamp, metric");
        sb.append(" ").append("ORDER BY timestamp");
        stmt = connection.prepareStatement(sb.toString());
        stmt.setString(1, argument.getAccountId());
        stmt.setString(2, argument.getApplicationId());
        stmt.setLong(3, start);
        stmt.setLong(4, end);
        stmt.setString(5, metric);
        LOG.trace("Timeseries query {}", stmt.toString());
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
        sb.append(" ").append("metric = ?") ;
        sb.append(" ").append("GROUP BY timestamp, metric");
        sb.append(" ").append("ORDER BY timestamp");
        stmt = connection.prepareStatement(sb.toString());
        stmt.setString(1, argument.getAccountId());
        stmt.setString(2, argument.getApplicationId());
        stmt.setString(3, argument.getFlowId());
        stmt.setLong(4, start);
        stmt.setLong(5, end);
        stmt.setString(6, metric);
        LOG.trace("Timeseries query {}", stmt.toString());
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
        sb.append(" ").append("metric = ?") ;
        sb.append(" ").append("GROUP BY timestamp, metric");
        sb.append(" ").append("ORDER BY timestamp");
        stmt = connection.prepareStatement(sb.toString());
        stmt.setString(1, argument.getAccountId());
        stmt.setString(2, argument.getApplicationId());
        stmt.setString(3, argument.getFlowId());
        stmt.setString(4, argument.getFlowletId());
        stmt.setLong(5, start);
        stmt.setLong(6, end);
        stmt.setString(7, metric);
        LOG.trace("Timeseries query {}", stmt.toString());
      }

      // Execute the query.
      rs = stmt.executeQuery();

      // Iterate through the points.
      while(rs.next()) {
        DataPoint.Builder dpb = new DataPoint.Builder(rs.getString("metric"));
        dpb.addTimestamp(rs.getLong("timestamp"));
        dpb.addValue(rs.getFloat("aggregate"));
        results.add(dpb.create());
      }
    } catch (SQLException e) {
      LOG.warn("Failed retrieving data for request {}. Reason : {}",
               argument.toString(), e.getMessage());
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
        LOG.warn("Failed closing recordset/statement/connection. Reason : " +
                   "{}", e.getMessage());
      }
    }
    return results;
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

  private void validateTimeseriesRequest(TimeseriesRequest request)
    throws MetricsServiceException {

    if(! request.isSetArgument()) {
      throw new MetricsServiceException("Flow arguments should be specified.");
    }

    if(! request.isSetMetrics()) {
      throw new MetricsServiceException("No metrics specified");
    }
  }

}
