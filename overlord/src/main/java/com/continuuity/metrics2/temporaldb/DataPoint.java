package com.continuuity.metrics2.temporaldb;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import java.util.Map;

/**
 * Implementation of a measurement represented as a datapoint in point of time.
 */
public final class DataPoint {
  /** Specifies the metric name. */
  private final String metric;

  /** Specifies the timestamp. */
  private final long timestamp;

  /** Specifies the value associated with metric. */
  private final double value;

  /** Specifies tags associated with metric. */
  private final Map<String, String> tags;

  /** Private constructor called only from builder */
  private DataPoint(final String metric, final long timestamp,
                    final double value, final Map<String, String> tags) {
    this.metric = metric;
    this.timestamp = timestamp;
    this.value = value;
    this.tags = tags;
  }

  /**
   * @return Name of the metric.
   */
  public String getMetric() {
    return metric;
  }

  /**
   * @return timestamp in epoch.
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * @return value associated with metric.
   */
  public double getValue() {
    return value;
  }

  /**
   * @return tags associated with metric.
   */
  public Map<String, String> getTags() {
    return tags;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(metric, timestamp, value, tags);
  }

  @Override
  public boolean equals(Object o) {
    if(o == null) {
      return false;
    }
    DataPoint other = (DataPoint) o;
    return Objects.equal(metric, other.metric) &&
      Objects.equal(value, other.value) &&
      Objects.equal(timestamp, other.timestamp) &&
      Objects.equal(tags, other.tags);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("metric", metric)
      .add("value", value)
      .add("timestamp", timestamp)
      .add("tags", tags)
      .toString();
  }

  /**
   * DataPoint builder.
   */
  public static class Builder {
    private final String metric;
    private long timestamp;
    private double value;
    private Map<String, String> tags = Maps.newHashMap();

    public Builder(String metric) {
      this.metric = metric;
    }

    /**
     * Adds a timestamp to metric.
     *
     * @param timestamp the metric was measured.
     * @return this object
     */
    public Builder addTimestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    /**
     * Adds a value to metric.
     *
     * @param value that was measured by metric.
     * @return this object.
     */
    public Builder addValue(double value) {
      this.value = value;
      return this;
    }

    public Builder addTag(String tagName, String tagValue) {
      tags.put(tagName, tagValue);
      return this;
    }

    public Builder addTags(Map<String, String> tags) {
      this.tags.putAll(tags);
      return this;
    }

    /**
     * @return Immutable instance of {@link DataPoint}
     */
    public DataPoint create() {
      DataPoint dp = new DataPoint(metric, timestamp, value, tags);
      return dp;
    }
  }
}
