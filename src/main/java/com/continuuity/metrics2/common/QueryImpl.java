package com.continuuity.metrics2.common;

import com.google.common.base.Function;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: nmotgi
 * Date: 10/7/12
 * Time: 5:45 PM
 * To change this template use File | Settings | File Templates.
 */
public class QueryImpl implements Query {
  private long startTime;
  private long endTime;
  private String metricName;
  private Map<String, String> tagFilter;
  private Function<DataPoint, Void> callback;

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public String getMetricName() {
    return metricName;
  }

  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  public Map<String, String> getTagFilter() {
    return tagFilter;
  }

  @Override
  public void setCallback(Function<DataPoint, Void> callback) {
    this.callback = callback;
  }

  @Override
  public Function<DataPoint, Void> getCallback() {
    return callback;
  }

  public void setTagFilter(Map<String, String> tagFilter) {
    this.tagFilter = tagFilter;
  }

}