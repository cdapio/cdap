package com.continuuity.metrics2.common;

import java.util.Map;

/**
 * Defines an interface for a datapoint in timeseries.
 */
public interface DataPoint {
  /**
   * @return Name of the metric.
   */
  public String getMetric();

  /**
   * @return timestamp in epoch.
   */
  public long getTimestamp();

  /**
   * @return value associated with metric.
   */
  public double getValue();

  /**
   * @return tags associated with metric.
   */
  public Map<String, String> getTags();
}
