package com.continuuity.overlord.metrics.service;

import com.continuuity.common.service.Stoppable;
import com.continuuity.overlord.metrics.stubs.FlowMetric;

/**
 *
 */
public interface FlowMonitorHandler extends Stoppable {
  public void add(FlowMetric metric);
}
