/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.collect;

import com.google.common.util.concurrent.Service;

/**
 * Service for collects and publishes metrics.
 */
public interface MetricsCollectionService extends Service {

  /**
   * Returns the metric collector for the give metric name.
   * @param metricName Name of the metric.
   * @return A {@link MetricsCollector} for emitting metrics for the given metric name.
   */
  MetricsCollector getCollector(String context, String metricName);
}
