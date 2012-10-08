package com.continuuity.metrics2.common;

import java.util.Map;

/**
 * Immutable implementation of {@link DataPoint} interface.
 */
public class DataPointImpl implements DataPoint {
  /** Name of the metric. */
  private final String metric;

  /** Timestamp the metric was measured. */
  private final long timestamp;

  /** Value of the metric. */
  private final double value;

  /** Tags associated with metric. */
  private final Map<String, String> tags;

  public DataPointImpl(final String metric, final long timestamp,
                       final double value, final Map<String, String> tags) {
    this.metric = metric;
    this.timestamp = timestamp;
    this.value = value;
    this.tags = tags;
  }
  /**
   * @return Name of the metric.
   */
  @Override
  public String getMetric() {
    return metric;
  }

  /**
   * @return timestamp in epoch.
   */
  @Override
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * @return value associated with metric.
   */
  @Override
  public double getValue() {
    return value;
  }

  /**
   * @return tags associated with metric.
   */
  @Override
  public Map<String, String> getTags() {
    return tags;
  }
}
