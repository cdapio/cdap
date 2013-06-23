package com.continuuity.common.metrics;

import com.continuuity.api.metrics.Metrics;

/**
 * Provides handy abstract implementation for metrics collector
 */
public abstract class AbstractCMetrcisBasedMetrics implements Metrics {
  private final CMetrics cMetrics;

  /**
   * Constructs the metrics
   * @param metricsGroup metrics group name
   */
  protected AbstractCMetrcisBasedMetrics(String metricsGroup) {
    this.cMetrics = new CMetrics(MetricType.FlowUser, metricsGroup);
  }

  @Override
  public void count(String counterName, int delta) {
    cMetrics.counter(counterName, delta);
  }
}
