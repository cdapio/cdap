package com.continuuity.overlord.metrics;

/**
 *
 */
public interface CMetrics {
  public CMetricsContext getMetricsMeta();
  public String getGroup();
  public String getType();
  public void incr(String metric);
  public void incr(String metric, long l);
}
