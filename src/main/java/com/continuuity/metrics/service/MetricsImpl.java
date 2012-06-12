package com.continuuity.metrics.service;

import com.continuuity.metrics.stubs.*;
import org.apache.thrift.TException;

import java.util.List;

/**
 *
 *
 */
class MetricsImpl implements FlowMonitor.Iface {
  private final MetricsHandler handler;

  public MetricsImpl(MetricsHandler handler) {
    this.handler = handler;
  }

  @Override
  public void add(FlowMetric metric) throws TException {
    handler.add(metric);
  }

  @Override
  public List<FlowState> getFlows(String accountId) throws TException {
    return handler.getFlows(accountId);
  }

  @Override
  public List<FlowRun> getFlowHistory(String accountId, String appId, String flowId) throws TException {
    return handler.getFlowHistory(accountId, appId, flowId);
  }

  /**
   * Returns definition of a flow.
   * NOTE: Version ID is not used for now, but we found out that in order to get the right definition,
   * we need to have the versionId.
   *
   * @param accountId
   * @param appId
   * @param flowId
   * @param versionId
   */
  @Override
  public String getFlowDefinition(String accountId, String appId, String flowId, String versionId) throws TException {
    return handler.getFlowDefinition(accountId, appId, flowId, versionId);
  }

  /**
   * Returns metric for a flow.
   *
   * @param accountId
   * @param appId
   * @param flowId
   * @param rid
   */
  @Override
  public List<Metric> getFlowMetrics(String accountId, String appId, String flowId, String rid) throws TException {
    return handler.getFlowMetric(accountId, appId, flowId, rid);
  }

}
