package com.continuuity.metrics2.common;

import com.google.common.base.Function;

import java.util.Map;

/**
 * Query to retrieve data from DB.
 */
public interface Query {
  public static final String WILDCARD = "*";
  public static final int WILDCARD_ID = -1;

  /**
   * Sets the start time of the graph.
   *
   * @param timestamp
   *            The start time, all the data points returned will have a
   *            timestamp greater than or equal to this one.
   * @throws IllegalArgumentException
   *             if timestamp is less than or equal to 0, or if it can't fit
   *             on 32 bits.
   * @throws IllegalArgumentException
   *             if {@code timestamp >= }{@link #getEndTime getEndTime}.
   */
  void setStartTime(long timestamp);

  /**
   * Returns the start time of the graph.
   *
   * @return A strictly positive integer.
   * @throws IllegalStateException
   *             if {@link #setStartTime} was never called on this instance
   *             before.
   */
  long getStartTime();

  /**
   * Sets the end time of the graph.
   *
   * @param timestamp
   *            The end time, all the data points returned will have a
   *            timestamp less than or equal to this one.
   * @throws IllegalArgumentException
   *             if timestamp is less than or equal to 0, or if it can't fit
   *             on 32 bits.
   * @throws IllegalArgumentException
   *             if {@code timestamp <= }{@link #getStartTime getStartTime}.
   */
  void setEndTime(long timestamp);

  /**
   * Returns the end time of the graph.
   * <p>
   * If {@link #setEndTime} was never called before, this method will
   * automatically execute {@link #setEndTime setEndTime}
   * {@code (System.currentTimeMillis() / 1000)} to set the end time.
   *
   * @return A strictly positive integer.
   */
  long getEndTime();

  /**
   * Sets the time series to the query.
   *
   * @param metric
   *            The metric to retreive from the TSDB.
   */
  void setMetricName(String metric);

  /**
   * @return Name of metric.
   */
  String getMetricName();

  /**
   * Sets tags to filter the retrieved metrics on.
   *
   * @param tags to be filtered on.
   */
  void setTagFilter(Map<String, String> tags);

  /**
   * @return tags set for filtering the metric.
   */
  Map<String, String> getTagFilter();

  void setCallback(Function<DataPoint, Void> callback);

  public Function<DataPoint, Void> getCallback();
}
