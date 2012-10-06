package com.continuuity.data.operation.executor.remote;

import com.continuuity.common.metrics.CMetrics;

public class MetricsHelper {

  private CMetrics metrics;
  private Class<?> scope;
  private long startTime;
  private String histogram;

  public MetricsHelper(CMetrics metrics, Class<?> scope,
                       String meter, String counter, String histogram) {
    this.metrics = metrics;
    this.scope = scope;
    this.startTime = System.currentTimeMillis();
    this.histogram = histogram;

    if (meter != null) metrics.meter(scope, meter, 1);
    if (counter != null) metrics.counter(scope, counter, 1);
  }

  public void success() {
    metrics.meter(scope, Constants.METRIC_SUCCESS, 1);
    if (histogram != null) {
      long latency = System.currentTimeMillis() - startTime;
      metrics.histogram(scope, histogram, latency);
    }
  }

  public void failure() {
    metrics.meter(scope, Constants.METRIC_FAILURE, 1);
  }

  public void finish(boolean success) {
    if (success)
      success();
    else
      failure();
  }

}
