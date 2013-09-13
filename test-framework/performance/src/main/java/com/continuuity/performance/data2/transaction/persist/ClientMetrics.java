package com.continuuity.performance.data2.transaction.persist;

import com.continuuity.performance.benchmark.BenchmarkMetric;
import com.google.common.base.Stopwatch;
import org.apache.hadoop.hbase.metrics.histogram.MetricsHistogram;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ClientMetrics {
  private BenchmarkMetric metrics;
  private MetricsHistogram writeLatencyMetrics;

  private String successCountKey;
  private String failedCountKey;

  private Stopwatch timer;

  public ClientMetrics(long agentId, BenchmarkMetric metrics, MetricsHistogram latencyMetrics) {
    this.metrics = metrics;
    this.writeLatencyMetrics = latencyMetrics;
    this.timer = new Stopwatch();
    this.successCountKey = String.format("agent.%d.success", agentId);
    this.failedCountKey = String.format("agent.%d.failed", agentId);
  }

  public void startTransaction() {
    timer.start();
  }

  public void finishTransaction() {
    timer.stop();
    metrics.increment(successCountKey, 1);
    writeLatencyMetrics.update(timer.elapsedTime(TimeUnit.MILLISECONDS));
    timer.reset();
  }

  public void failTransaction() {
    timer.stop();
    metrics.increment(failedCountKey, 1);
    writeLatencyMetrics.update(timer.elapsedTime(TimeUnit.MILLISECONDS));
    timer.reset();
  }
}
