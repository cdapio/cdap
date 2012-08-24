package com.continuuity.data.operation.executor.remote;

import com.continuuity.metrics2.api.CMetrics;

public class MetricsHelper {

  private CMetrics metrics;
  private Class<?> scope;
  private long startTime;

  public MetricsHelper(CMetrics metrics,
                       Class<?> scope, String meter, String counter) {
    this.metrics = metrics;
    this.scope = scope;
    this.startTime = System.currentTimeMillis();

    if (meter != null) metrics.meter(scope, meter, 1);
    if (counter != null) metrics.counter(scope, counter, 1);
  }

  public void finish(String status, String histogram) {
    long latency = System.currentTimeMillis() - startTime;

    if (status != null) metrics.meter(scope, status, 1);
    if (histogram != null) metrics.histogram(scope, histogram, latency);
  }

}
