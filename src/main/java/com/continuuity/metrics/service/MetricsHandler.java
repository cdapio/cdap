package com.continuuity.metrics.service;

import com.continuuity.metrics.stubs.*;

import java.util.List;

/**
 *
 */
public interface MetricsHandler {

  /**
   * Initializes metrics handler.
   *
   * @param uri Specifies URI to storage.
   * @throws Exception
   */
  public void init(final String uri) throws Exception;

  /**
   * Adds a metric for a flow.
   *
   * @param metric to be added.
   */
  public void add(FlowMetric metric);

  /**
   * Returns list of flows and their state for a given account id.
   *
   * @param accountId specifying the flows to be returned.
   *
   * @return list of flow state.
   */
  List<FlowState> getFlows(String accountId);

  /**
   * Returns a list of runs for a given flow.
   *
   * @param accountId for which the flows belong to.
   * @param appId  to which the flows belong to.
   * @param flowId to which the flows belong to.
   *
   * @return a list of flow runs
   */
  public List<FlowRun> getFlowHistory(String accountId, String appId,
                                      String flowId);

  /**
   * Returns the flow definition.
   *
   * @param accountId  for which the flows belong to.
   * @param appId  to which the flows belong to.
   * @param flowId  to which the flows belong to.
   * @param versionId of the flow for which the definition needs to be retrieved
   *
   * @return A String representation of the flow definition
   */
  String getFlowDefinition(String accountId, String appId, String flowId,
                           String versionId);

  /**
   * Returns metrics for a given a run id.
   *
   * @param accountId  for which the flows belong to.
   * @param appId  to which the flows belong to.
   * @param flowId  to which the flows belong to.
   * @param rid run id of the flow.
   *
   * @return A list of Metrics for the given run id
   */
  List<Metric> getFlowMetric(String accountId, String appId, String
                             flowId, String rid);
}
