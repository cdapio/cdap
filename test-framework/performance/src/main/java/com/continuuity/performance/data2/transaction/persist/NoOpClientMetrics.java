package com.continuuity.performance.data2.transaction.persist;

import com.continuuity.performance.benchmark.BenchmarkMetric;
import org.apache.hadoop.hbase.metrics.histogram.MetricsHistogram;

/**
 *
 */
public class NoOpClientMetrics extends ClientMetrics {


  public NoOpClientMetrics(long agentId, BenchmarkMetric metrics, MetricsHistogram latencyMetrics) {
    super(agentId, metrics, latencyMetrics);
  }

  @Override
  public void startTransaction() {
    // no-op
  }

  @Override
  public void finishTransaction() {
    // no-op
  }

  @Override
  public void failTransaction() {
    // no-op
  }
}
