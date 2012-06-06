package com.continuuity.overlord.metrics.service;

import com.continuuity.overlord.metrics.stubs.FlowMetric;
import com.continuuity.overlord.metrics.stubs.FlowMonitor;
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
