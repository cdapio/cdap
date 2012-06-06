package com.continuuity.metrics.internal;

import com.continuuity.metrics.CMetrics;
import com.google.common.collect.ImmutableMap;

/**
 *
 */
public final class FlowMetricFactory {
  public static CMetrics newFlowMetrics(ImmutableMap<String, String> fields, String group, String type) {
    return new FlowMetrics(new FlowMetricsContext(fields), group, type);
  }
}
