package com.continuuity.data2.transaction.metrics;

/**
 * Metrics Collector Class, to emit Transaction Related Metrics.
 * Note: This default implementation is a no-op and doesn't emit any metrics
 */
public class TxMetricsCollector {

  /**
   * Send metric value for a given metric, identified by the metricsName, can also be tagged
   */
  public void gauge(String metricName, int value, String...tags) {
   //no-op
  }

}
