package com.continuuity.metrics.service;

import com.continuuity.common.service.Stoppable;
import com.continuuity.metrics.stubs.FlowMetric;

/**
 *
 */
public interface FlowMonitorHandler extends Stoppable {
  public void add(FlowMetric metric);
}
