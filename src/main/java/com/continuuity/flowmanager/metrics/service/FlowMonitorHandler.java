package com.continuuity.flowmanager.metrics.service;

import com.continuuity.common.service.Stoppable;
import com.continuuity.flowmanager.metrics.stubs.FlowMetric;

/**
 *
 */
public interface FlowMonitorHandler extends Stoppable {
  public void add(FlowMetric metric);
}
