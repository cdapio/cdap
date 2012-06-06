package com.continuuity.metrics.service;

import com.continuuity.common.service.Stoppable;
import com.continuuity.metrics.stubs.FlowEvent;
import com.continuuity.metrics.stubs.FlowMetric;
import com.continuuity.metrics.stubs.Metric;

import java.util.List;

/**
 *
 */
public interface FlowMonitorHandler extends Stoppable {
  public void add(FlowMetric metric);
  public List<FlowEvent> getFlowHistory(String accountId, String app, String flow);
  List<Metric> getFlowMetric(String accountId, String app, String flow, String rid);
}
