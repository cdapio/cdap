package com.continuuity.flowmanager.metrics.internal;

import com.continuuity.flowmanager.metrics.CMetrics;
import com.google.common.collect.ImmutableMap;

/**
 *
 */
public final class FlowMetricFactory {
  public static CMetrics newFlowMetrics(ImmutableMap<String, String> fields, String group, String type) {
    return new FlowMetrics(new FlowMetricsContext(fields), group, type);
  }
}
