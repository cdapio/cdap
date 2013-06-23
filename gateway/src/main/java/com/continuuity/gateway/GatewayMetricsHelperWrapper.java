package com.continuuity.gateway;

import com.continuuity.common.metrics.MetricsHelper;

/**
 * Wraps {@link MetricsHelper} to propagate counter metrics collection to gatewayMetrics.
 * Does NOT change the behavior of provided {@link MetricsHelper}.
 */
public class GatewayMetricsHelperWrapper extends MetricsHelper {
  private final GatewayMetrics gatewayMetrics;

  public GatewayMetricsHelperWrapper(MetricsHelper other, GatewayMetrics gatewayMetrics) {
    super(other);
    this.gatewayMetrics = gatewayMetrics;
  }

  @Override
  protected void count(String metricWithStatus, long count) {
    super.count(metricWithStatus, count);
    // in unit-tests it can be not initialized
    if (gatewayMetrics != null) {
      gatewayMetrics.count(metricWithStatus, count);
    }
  }
}
