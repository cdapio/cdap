package com.continuuity.data2.transaction.metrics;

import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.metrics.MetricsScope;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reactor implementation for TxMetricsCollector
 */
public class ReactorTxMetricsCollector extends TxMetricsCollector {
  MetricsCollector metricsCollector;
  private static final Logger LOG = LoggerFactory.getLogger(ReactorTxMetricsCollector.class);

  @Inject
  public ReactorTxMetricsCollector(MetricsCollectionService metricsCollectionService) {
    metricsCollector = metricsCollectionService.getCollector(MetricsScope.REACTOR, "transactions", "0");
  }

  @Override
  public void gauge(String metricName, int value, String...tags) {
    metricsCollector.gauge(metricName, value, tags);
  }

}
