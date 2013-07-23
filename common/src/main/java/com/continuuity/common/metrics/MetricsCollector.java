/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.metrics;

/**
 * A MetricCollector allows client publish counter metrics.
 */
public interface MetricsCollector {

  /**
   * Log a metric value at the current time.
   * @param metricName Name of the metric.
   * @param value value of the metric.
   * @param tags Tags associated with the metric.
   */
  void gauge(String metricName, int value, String...tags);
}
