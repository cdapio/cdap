package com.continuuity.metrics;

/**
 *
 */
public interface CMetricsContext {
  public String getScope();
  public String getName(String metric);
  public String getField(String name);
}
