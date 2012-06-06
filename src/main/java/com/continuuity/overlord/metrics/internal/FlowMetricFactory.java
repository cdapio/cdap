package com.continuuity.overlord.metrics.internal;

import com.continuuity.overlord.metrics.CMetrics;
import com.google.common.collect.ImmutableMap;

/**
 *
 */
public final class FlowMetricFactory {
  public static CMetrics newFlowMetrics(ImmutableMap<String, String> fields, String group, String type) {
    return new FlowMetrics(new FlowMetricsContext(fields), group, type);
  }
}
