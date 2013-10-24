package com.continuuity.performance.data2.transaction.persist;

import com.continuuity.performance.benchmark.BenchmarkMetric;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
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
  private Stopwatch totalTimer;

  public ClientMetrics(long agentId, BenchmarkMetric metrics, MetricsHistogram latencyMetrics) {
    this.metrics = metrics;
    this.writeLatencyMetrics = latencyMetrics;
    this.timer = new Stopwatch();
    this.totalTimer = new Stopwatch();
    this.successCountKey = String.format("agent.%d.success", agentId);
    this.failedCountKey = String.format("agent.%d.failed", agentId);
  }

  public void startTransaction() {
    timer.start();
    totalTimer.start();
  }

  public void finishTransaction() {
    timer.stop();
    totalTimer.stop();
    metrics.increment(successCountKey, 1);
    writeLatencyMetrics.update(timer.elapsedTime(TimeUnit.MICROSECONDS));
    timer.reset();
  }

  public void failTransaction() {
    timer.stop();
    totalTimer.stop();
    metrics.increment(failedCountKey, 1);
    writeLatencyMetrics.update(timer.elapsedTime(TimeUnit.MICROSECONDS));
    timer.reset();
  }

  public Long getSuccessCount() {
    return metrics.list(Sets.newHashSet(successCountKey)).get(successCountKey);
  }

  public Long getFailureCount() {
    return metrics.list(Sets.newHashSet(failedCountKey)).get(failedCountKey);
  }

  public BenchmarkMetric getMetrics() {
    return metrics;
  }

  /**
   * Returns total time in milliseconds
   */
  public Stopwatch getTotalTimer() {
    return totalTimer;
  }
}
