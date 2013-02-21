package com.continuuity.api.common;

/**
 * Metrics interface
 */
@Deprecated
public interface CMetricsIntfc {
  /**
   * Set the counter for specified key with the value
   * @param metricName String metrics key
   * @param value value to be set
   */
  public void counter(String metricName, long value);
}
