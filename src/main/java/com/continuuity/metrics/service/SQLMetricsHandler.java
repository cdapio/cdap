package com.continuuity.metrics.service;

import com.continuuity.metrics.stubs.*;
import com.continuuity.observer.StateChangeType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.sql.*;
import java.util.List;
import java.util.Map;

/**
 *
 *
 */
public class SQLMetricsHandler implements MetricsHandler {
  private static final Logger Log = LoggerFactory.getLogger(SQLMetricsHandler.class);
  private final String url;
  private final Connection connection;
  private volatile boolean running;

  @Inject
  public SQLMetricsHandler(@Named("Flow Monitor JDBC URL") final String url) throws SQLException {
    this.url = url;
    connection = DriverManager.getConnection(url, "sa", "");
    initialization();
    running = false;
  }

  public Connection getConnection() {
    return connection;
  }

  public void initialization() {
    try {
      connection.prepareStatement(
        "CREATE TABLE flow_metrics (id BIGINT IDENTITY, timestamp INTEGER, accountid VARCHAR, " +
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
      stmt.setInt(1, metric.getTimestamp());
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

  /**
   * @param accountId
   * @param app
   * @param flow
   * @param rid
   * @return
   */
  @Override
  public List<Metric> getFlowMetric(String accountId, String app, String flow, String rid) {
    List<Metric> result = Lists.newArrayList();

    String maxTimeSQL = "SELECT MAX(timestamp) as maxtimestamp FROM flow_metrics " +
      "WHERE accountId = ? AND app = ? AND flow = ? AND rid = ?";
    int maxTimestamp = -1;
    try {
      PreparedStatement maxTimeStmt = connection.prepareStatement(maxTimeSQL);
      maxTimeStmt.setString(1, accountId);
      maxTimeStmt.setString(2, app);
      maxTimeStmt.setString(3, flow);
      maxTimeStmt.setString(4, rid);
      ResultSet rs = maxTimeStmt.executeQuery();
      rs.next();
      maxTimestamp = rs.getInt("maxtimestamp");
    } catch (SQLException e) {
      Log.warn("Unable to retrieve max timestamp for application '{}', Flow '{}', Run ID '{}'" +
        new Object[]{app, flow, rid});
      return result;
    }

    if(maxTimestamp == -1) {
      Log.warn("Unable to find max timestamp for application '{}', Flow '{}', Run ID '{}'" +
        new Object[]{app, flow, rid});
      return result;
    }

    String sql = "SELECT flowlet, metric, rid, SUM(value) AS total FROM flow_metrics WHERE accountId = ? AND app = ? AND " +
      "flow = ? AND rid = ? AND timestamp = ? GROUP by flowlet, metric, rid";
    try {
      PreparedStatement stmt = connection.prepareStatement(sql);
      stmt.setString(1, accountId);
      stmt.setString(2, app);
      stmt.setString(3, flow);
      stmt.setString(4, rid);
      stmt.setInt(5, maxTimestamp);
      ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        Metric metric = new Metric();
        metric.setId(rs.getString("flowlet"));
        metric.setType(MetricType.FLOWLET);
        metric.setName(rs.getString("metric"));
        metric.setValue(rs.getLong("total"));
        result.add(metric);
      }
    } catch (SQLException e) {
      Log.warn("Unable to retrieve flow metrics. Application '{}', Flow '{}', Run ID '{}'",
        new Object[]{app, flow, rid});
    }
    return result;
  }

  /**
   * FIXME: This was done in hurry and can be written in a much better way.
   *
   * @param accountId
   * @return
   */
  @Override
  public List<FlowState> getFlows(String accountId) {
    Map<String, Integer> started = Maps.newHashMap();
    Map<String, Integer> stopped = Maps.newHashMap();
    Map<String, Integer> runs = Maps.newHashMap();
    Map<String, Integer> deployed = Maps.newHashMap();
    Map<String, Integer> states = Maps.newHashMap();

    List<FlowState> result = Lists.newArrayList();
    String sql = "SELECT id, timestamp, application, flow, state " +
      "FROM flow_state WHERE account = ? ORDER by id";
    try {
      PreparedStatement stmt = connection.prepareStatement(sql);
      stmt.setString(1, accountId);
      ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        String app = rs.getString("application");
        String flow = rs.getString("flow");
        String appFlow = String.format("%s.%s", app, flow);
        Integer timestamp = rs.getInt("timestamp");
        int state = rs.getInt("state");

        if (!deployed.containsKey(appFlow)) {
          deployed.put(appFlow, 1);
          FlowState status = new FlowState();
          status.setApplicationId(rs.getString("application"));
          status.setFlowId(rs.getString("flow"));
          status.setCurrentState(StateChangeType.DEPLOYED.name());
          status.setLastStarted(-1);
          status.setLastStopped(-1);
          status.setRuns(0);
          result.add(status);
        }

        /** Flow has been deleted then it should not be included. */
        if(state == StateChangeType.DELETED.getType()) {
          started.remove(appFlow);
          stopped.remove(appFlow);
          runs.remove(appFlow);
          states.remove(appFlow);
        } else if (state == StateChangeType.STARTING.getType() || state == StateChangeType.RUNNING.getType()) {
          started.put(appFlow, timestamp);
        } else if (state == StateChangeType.STOPPING.getType()
          || state == StateChangeType.STOPPED.getType() || state == StateChangeType.FAILED.getType()) {
          stopped.put(appFlow, timestamp);
          if (runs.containsKey(flow)) {
            int run = runs.get(flow).intValue();
            runs.put(appFlow, run + 1);
          } else {
            runs.put(appFlow, 1);
          }
        }
        states.put(appFlow, state);
      }
    } catch (SQLException e) {
      Log.error("Unable to retrieve information about flows for account {}. Reason : {}.", accountId, e.getMessage());
    }

    for (FlowState state : result) {
      String flow = state.getFlowId();
      String app = state.getApplicationId();
      String appFlow = String.format("%s.%s", app, flow);

      if (started.containsKey(appFlow)) {
        state.setLastStarted(started.get(appFlow));
      } else {
        /* Flow might have been deployed. */
        result.remove(state);
        continue;
      }
      if (stopped.containsKey(appFlow)) {
        state.setLastStopped(stopped.get(appFlow));
      }
      if (runs.containsKey(appFlow)) {
        state.setRuns(runs.get(appFlow));
      }
      if (states.containsKey(appFlow)) {
        int i = states.get(appFlow);
        state.setCurrentState(StateChangeType.value(i).name());
      }
    }
    return result;
  }


  /**
   * FIXME : I am probably most duplicate of getFlows - Refactor me.
   *
   * @param accountId for which the flows belong to.
   * @param appId  to which the flows belong to.
   * @param flowId is the id of the flow runs to be returned.
   * @return
   */
  @Override
  public List<FlowRun> getFlowHistory(String accountId, String appId, String flowId) {
    Map<String, Integer> started = Maps.newHashMap();
    Map<String, Integer> stopped = Maps.newHashMap();
    Map<String, Integer> states = Maps.newHashMap();

    String sql = "SELECT id, timestamp, runid, state FROM flow_state WHERE account = ? AND application = ? " +
      "AND flow = ? ORDER by id";

    List<FlowRun> runs = Lists.newArrayList();
    try {
      PreparedStatement stmt = connection.prepareStatement(sql);
      stmt.setString(1, accountId);
      stmt.setString(2, appId);
      stmt.setString(3, flowId);
      ResultSet rs = stmt.executeQuery();
      while(rs.next()) {
        String rid = rs.getString("runid");
        if(rid == null) {
          continue;
        }
        int state = rs.getInt("state");

        if(state == StateChangeType.DEPLOYED.getType()) {
          continue;
        }

        int timestamp = rs.getInt("timestamp");

        if(! started.containsKey(rid))  {
          FlowRun run = new FlowRun();
          run.setStartTime(-1);
          run.setEndTime(-1);
          run.setRunId(rid);
          run.setEndStatus("NA");
          runs.add(run);
        }

        if(state == StateChangeType.STARTING.getType() || state == StateChangeType.RUNNING.getType()) {
          started.put(rid, timestamp );
        }

        if(state == StateChangeType.STOPPING.getType() || state == StateChangeType.STOPPED.getType()
          || state == StateChangeType.FAILED.getType()) {
          stopped.put(rid, timestamp );
        }

        states.put(rid, state);
      }

      for(FlowRun run : runs) {
        String runId = run.getRunId();
        if(started.containsKey(runId)) {
          run.setStartTime(started.get(runId));
        }
        if(stopped.containsKey(runId)) {
          run.setEndTime(stopped.get(runId));
        }
        if(states.containsKey(runId)) {
          run.setEndStatus(StateChangeType.value(states.get(runId)).name());
        }
      }
    } catch (SQLException e) {
      Log.error("Unable to get flow run for account {}, application {}, flow {}. Reason : {}", new Object[] {
        accountId, appId, flowId, e.getMessage()
      });
    }
    return runs;
  }

  /**
   * Returns the flow definition.
   *
   * @param accountId
   * @param appId
   * @param flowId
   * @param versionId
   * @return
   */
  @Override
  public String getFlowDefinition(String accountId, String appId, String flowId, String versionId) {
    String sql = "SELECT payload FROM flow_state WHERE account = ? AND application = ? " +
      "AND flow = ? AND state = 1 ORDER by timestamp DESC limit 1";

    String definition = null;
    try {
      PreparedStatement stmt = connection.prepareStatement(sql);
      stmt.setString(1, accountId);
      stmt.setString(2, appId);
      stmt.setString(3, flowId);
      ResultSet rs = stmt.executeQuery();
      rs.next();
      definition = rs.getString("payload");
    } catch (SQLException e) {
      Log.error("Unable to get flow run for account {}, application {}, flow {}. Reason : {}", new Object[] {
        accountId, appId, flowId, e.getMessage()
      });
    }
    return definition;
  }


}
