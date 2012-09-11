package com.continuuity.metrics2.frontend;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.metrics2.common.DBUtils;
import com.continuuity.metrics2.stubs.*;
import com.continuuity.metrics2.stubs.MetricsFrontendService;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.thrift.TException;
import org.mortbay.log.Log;

import javax.annotation.Nullable;
import java.sql.*;
import java.util.Iterator;
import java.util.List;

/**
 * MetricsService provides a readonly service for metrics.
 * It's a implementation that reads from the SQL supported DB.
 */
public class MetricsFrontendServiceImpl
  implements MetricsFrontendService.Iface {

  /**
   * Connection string to connect to database.
   */
  private String connectionUrl;

  /**
   * Type of Database we are configured with.
   */
  private DBUtils.DBType type;

  /**
   * TODO: Move this to use
   * {@link com.continuuity.metrics2.common.DBConnectionPoolManager}  for
   * managing connections to the DB.
   */
  private Connection connection;

  public MetricsFrontendServiceImpl(CConfiguration configuration)
    throws ClassNotFoundException, SQLException {
    this.connectionUrl
      = configuration.get(Constants.CFG_METRICS_CONNECTION_URL,
                          Constants.DEFAULT_METIRCS_CONNECTION_URL);
    this.type = DBUtils.loadDriver(connectionUrl);
    DBUtils.createMetricsTables(getConnection(), this.type);
  }

  /**
   * @return a {@link java.sql.Connection} based on the <code>connectionUrl</code>
   * @throws java.sql.SQLException thrown in case of any error.
   */
  private synchronized Connection getConnection() throws SQLException {
    if(connection == null) {
      connection = DriverManager.getConnection(connectionUrl);
    }
    return connection;
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

    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = getConnection().prepareStatement(sql);
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
      if(stmt != null) {
        try {
          stmt.close();
        } catch (SQLException e) {
          Log.warn("Failed to close prepared statement. Reason : {}",
                   e.getMessage());
        }
      }
      if(rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {
          Log.warn("Failed to close record set. Reason : {}", e.getMessage());
        }
      }
    }

    return results;
  }

}
