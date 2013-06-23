package com.continuuity.metrics2.temporaldb;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Query to retrieve data from DB.
 * Query.Builder(metricName).from(startTime).to(endTime).filterByTag(A,B).filterByTags(
 */
public final class Query {
  /** Start time, all points will be greater than or equal */
  private long startTime;

  /** End time, all points will be less than or equal. */
  private long endTime = Long.MIN_VALUE;

  /** Name of metric. */
  private String metricName;

  /** Tags to be used to filter. */
  private Map<String, String> tagFilter = Maps.newHashMap();

  /** Specifies the wild card character to be used for filtering on tags. */
  public static final String WILDCARD = "*";

  /** Specifies the ID for the wildcard. */
  public static final int WILDCARD_ID = -1;

  private Query(String metricName, long startTime, long endTime,
                Map<String, String> tags)  {
    this.metricName = metricName;
    this.startTime = startTime;
    this.endTime = endTime;
    this.tagFilter = tags;
  }
  /**
   * Returns the start time of the graph.
   *
   * @return A strictly positive integer.
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * Returns the end time of the graph.
   * <p>
   * If end time was not set, this method will automatically execute
   * {@code (System.currentTimeMillis() / 1000)} to set the end time.
   *
   * @return A strictly positive integer.
   */
  public long getEndTime() {
    if(endTime == Long.MIN_VALUE) {
      endTime = System.currentTimeMillis()/1000;
    }
    return endTime;
  }

  /**
   * @return Name of metric.
   */
  public String getMetricName() {
    return metricName;
  }

  /**
   * @return tags set for filtering the metric.
   */
  public Map<String, String> getTagFilter() {
    return tagFilter;
  }

  /**
   * Query q = Query.select("processed.count")
   *                .from(ts)
   *                .to(ts)
   *                .has("A", "B")
   *                .create();
   */
  public static class select {
    private final String metricName;
    private long endTime = System.currentTimeMillis()/1000;
    private long startTime = endTime - 5; // 5 seconds behind.
    private Map<String, String> tags = null;

    public select(String metricName) {
      this.metricName = metricName;
    }

    public select from(long startTime) {
      this.startTime = startTime;
      return this;
    }

    public select to(long endTime) {
      this.endTime = endTime;
      return this;
    }

    public select and() {
      if(tags == null) {
        tags = Maps.newHashMap();
      }
      return this;
    }

    public select has(String tagName, String tagValue) {
      this.tags.put(tagName, tagValue);
      return this;
    }

    public select has(Map<String, String> tags) {
      this.tags.putAll(tags);
      return this;
    }

    public Query create() {
      Query q = new Query(metricName, startTime, endTime, tags);
      return q;
    }

  }
}
