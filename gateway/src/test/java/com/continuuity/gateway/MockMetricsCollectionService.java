package com.continuuity.gateway;

import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.metrics.MetricsScope;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.Executor;

/**
 * Metrics collection service for tests.
 */
public class MockMetricsCollectionService implements MetricsCollectionService {
  private final Table<String, String, Integer> metrics = HashBasedTable.create();

  @Override
  public MetricsCollector getCollector(MetricsScope scope, String context, String runId) {
    return new MockMetricsCollector(context);
  }

  @Override
  public ListenableFuture<State> start() {
    return Futures.immediateCheckedFuture(null);
  }

  @Override
  public State startAndWait() {
    return State.RUNNING;
  }

  @Override
  public boolean isRunning() {
    return true;
  }

  @Override
  public State state() {
    return State.RUNNING;
  }

  @Override
  public ListenableFuture<State> stop() {
    return Futures.immediateCheckedFuture(null);
  }

  @Override
  public State stopAndWait() {
    return State.TERMINATED;
  }

  @Override
  public void addListener(Listener listener, Executor executor) {
    // no-op
  }

  public synchronized int getMetrics(String context, String metricName) {
    Integer val = metrics.get(context, metricName);
    return val == null ? 0 : val;
  }

  private class MockMetricsCollector implements MetricsCollector {
    private final String context;

    private MockMetricsCollector(String context) {
      this.context = context;
    }

    @Override
    public void gauge(String metricName, int value, String... tags) {
      synchronized (MockMetricsCollectionService.this) {
        Integer v = metrics.get(context, metricName);
        metrics.put(context, metricName, v == null ? value : v + value);
      }
    }
  }
}
