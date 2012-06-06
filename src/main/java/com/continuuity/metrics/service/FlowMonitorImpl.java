package com.continuuity.metrics.service;

import com.continuuity.metrics.stubs.FlowEvent;
import com.continuuity.metrics.stubs.FlowMonitor;
import com.continuuity.metrics.stubs.FlowMetric;
import com.continuuity.metrics.stubs.Metric;
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
  public List<FlowEvent> getFlowHistory(String accountId, String app, String flow) throws TException {
    return handler.getFlowHistory(accountId, app, flow);
  }

  @Override
  public List<Metric> getFlowMetric(String accountId, String app, String flow, String rid) throws TException {
    return handler.getFlowMetric(accountId, app, flow, rid);
  }
}
