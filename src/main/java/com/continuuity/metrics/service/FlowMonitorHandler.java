package com.continuuity.metrics.service;

import com.continuuity.metrics.stubs.FlowEvent;
import com.continuuity.metrics.stubs.FlowMetric;
import com.continuuity.metrics.stubs.FlowState;
import com.continuuity.metrics.stubs.Metric;

import java.util.List;

/**
 *
 */
public interface FlowMonitorHandler {
  public void add(FlowMetric metric);

  public List<FlowEvent> getFlowHistory(String accountId, String app, String flow);

  List<Metric> getFlowMetric(String accountId, String app, String flow, String rid);

  List<FlowState> getFlows(String accountId);
}
