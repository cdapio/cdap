package com.continuuity.metrics2.collector.plugins;

import com.continuuity.common.builder.BuilderException;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;

import java.util.Iterator;

/**
 * Given a flow metric name, this class contains the constituents for
 * different parts of the flow.
 * <p>
 *   A metric name is of the form
 *   <code>
 *     accountId.applicationId.flowId.runId.flowletId.instanceId
 *   </code>
 * </p>
 */
final class FlowMetricElements {
  private static final Splitter FLOW_ELEMENTS_SPLITTER
    = Splitter.on(".").trimResults().omitEmptyStrings();

  /**
   * Account Id of the flow.
   */
  private String accountId;

  /**
   * Application the flow belongs to.
   */
  private String applicationId;

  /**
   * Id of the flow.
   */
  private String flowId;

  /**
   * Run Id of the flow.
   */
  private String runId;

  /**
   * Id of the flowlet within a flow.
   */
  private String flowletId;

  /**
   * Instance Id of the flowlet.
   */
  private int instanceId;

  /**
   * Metric name.
   */
  private String metric;

  private FlowMetricElements() {

  }

  /**
   * @return Account Id associated with the flow.
   */
  public String getAccountId() {
    return accountId;
  }

  /**
   * @return Application Id the flow is associated with.
   */
  public String getApplicationId() {
    return applicationId;
  }

  /**
   * @return Id of the flow.
   */
  public String getFlowId() {
    return flowId;

  }

  /**
   * @return Run Id of the flow for which we received metric.
   */
  public String getRunId() {
    return runId;
  }

  /**
   * @return Id of the flowlet within the flow.
   */
  public String getFlowletId() {
    return flowletId;
  }

  /**
   * @return Instance id of the flowlet within a flowlet group and flow.
   */
  public int getInstanceId() {
    return instanceId;
  }

  /**
   * @return name of metric.
   */
  public String getMetric() {
    return metric;
  }

  /**
   * @return String respresentation of {@link FlowMetricElements}
   */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("Account Id", accountId)
      .add("Application Id", applicationId)
      .add("Flow Id", flowId)
      .add("Run Id", runId)
      .add("Flowlet Id", flowletId)
      .add("Instance Id", instanceId)
      .add("Metric Name", metric)
      .toString();
  }

  public static class Builder {
    private String metricName;

    public Builder(String metricName) {
      this.metricName = metricName;
    }

    public FlowMetricElements create() throws BuilderException {
      Iterator<String> constituents =
          FLOW_ELEMENTS_SPLITTER.split(metricName).iterator();

      FlowMetricElements fmc = new FlowMetricElements();

      // Read the elements in the right order.
      // Order: accountId.applicationId.flowId.runId.flowletId.instanceId
      fmc.accountId = get(constituents);
      fmc.applicationId = get(constituents);
      fmc.flowId = get(constituents);
      fmc.runId = get(constituents);
      fmc.flowletId = get(constituents);
      String instanceIdStr = get(constituents);
      try {
        fmc.instanceId = Integer.parseInt(instanceIdStr);
      } catch (NumberFormatException e) {
        throw new BuilderException("Instance ID is not an integer. Metric " +
          metricName);
      }

      String metric = "";
      while (constituents.hasNext()) {
        String name = constituents.next();
        if (!constituents.hasNext()){
          metric = metric + name;
        } else {
          metric = metric + name + ".";
        }
      }
      fmc.metric = metric;
      return fmc;
    }

    private String get(Iterator<String> constituents)
      throws BuilderException {
      if (!constituents.hasNext()) {
        throw new BuilderException("Improper metric name " + metricName);
      }
      return constituents.next();
    }
  }
}
