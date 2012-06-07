package com.continuuity.metrics.service;

import com.continuuity.common.service.Stoppable;
import com.continuuity.metrics.stubs.*;

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
