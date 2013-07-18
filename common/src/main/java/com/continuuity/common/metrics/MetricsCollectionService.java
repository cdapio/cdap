/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.metrics;

import com.google.common.util.concurrent.Service;

/**
 * Service for collects and publishes metrics.
 */
public interface MetricsCollectionService extends Service {

  /**
   * Returns the metric collector for the given context.
   * @param context Name of the context that generating the metric.
   * @param runId The Id fo the given run that generating the metric.
   * @return A {@link MetricsCollector} for emitting metrics.
   */
  MetricsCollector getCollector(MetricsScope scope, String context, String runId);
}
