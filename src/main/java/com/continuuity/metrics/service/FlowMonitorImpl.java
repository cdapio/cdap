package com.continuuity.metrics.service;

import com.continuuity.metrics.stubs.FlowMonitor;
import com.continuuity.metrics.stubs.FlowMetric;
import org.apache.thrift.TException;

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
}
