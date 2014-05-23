package com.continuuity.common.metrics;

import com.google.common.util.concurrent.AbstractIdleService;

/**
 * No-op, to be used in unit-tests
 */
public class NoOpMetricsCollectionService extends AbstractIdleService implements MetricsCollectionService {

  @Override
  protected void startUp() throws Exception {
    // no-op
  }

  @Override
  protected void shutDown() throws Exception {
    // no-op
  }

  @Override
  public MetricsCollector getCollector(MetricsScope scope, String context, String runId) {
    return new MetricsCollector() {
      @Override
      public void gauge(String metricName, int value, String... tags) {
        // no-op
      }
    };
  }
}
