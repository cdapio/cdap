package com.continuuity.metrics.service;

import com.continuuity.metrics.stubs.*;
import org.apache.thrift.TException;

import java.util.List;

/**
 *
 *
 */
class FlowMonitorImpl implements FlowMonitor.Iface {
  private final FlowMonitorHandler handler;

  public FlowMonitorImpl(FlowMonitorHandler handler) {
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

}
