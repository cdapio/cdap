package com.continuuity.data2.transaction.metrics;

/**
 * Metrics Collector Class, to emit Transaction Related Metrics
 */
public class TxMetricsCollector {

  /**
   * Send metric value for a given metric, identified by metricsName
   * @param metricName
   * @param value
   * @param tags
   */
  public void gauge(String metricName, int value, String...tags) {
   //no-op
  }

}
