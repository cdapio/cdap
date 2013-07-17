package com.continuuity.app.metrics;

import com.continuuity.api.metrics.Metrics;
import com.continuuity.api.metrics.MetricsCollectionService;
import com.continuuity.api.metrics.MetricsCollector;

/**
 * Base class for defining implementation of {@link Metrics} for different type of runtime context.
 * Metrics will be emitted through {@link MetricsCollectionService}.
 */
public abstract class AbstractProgramMetrics implements Metrics {

  private final MetricsCollector metricsCollector;

  protected AbstractProgramMetrics(MetricsCollector metricsCollector) {
    this.metricsCollector = metricsCollector;
  }

  @Override
  public void count(String counterName, int delta) {
    metricsCollector.gauge(counterName, delta);
  }
}
