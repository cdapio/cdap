package com.continuuity.metrics.data;

import java.util.Iterator;

/**
 *
 */
public final class MetricsScanResult implements Iterable<TimeValue> {

  private final String context;
  private final String runId;
  private final String metric;
  private final String tag;
  private final Iterable<TimeValue> timeValues;

  public MetricsScanResult(String context, String runId, String metric, String tag, Iterable<TimeValue> timeValues) {
    this.context = context;
    this.runId = runId;
    this.metric = metric;
    this.tag = tag;
    this.timeValues = timeValues;
  }

  public String getContext() {
    return context;
  }

  public String getRunId() {
    return runId;
  }

  public String getMetric() {
    return metric;
  }

  public String getTag() {
    return tag;
  }

  @Override
  public Iterator<TimeValue> iterator() {
    return timeValues.iterator();
  }
}
