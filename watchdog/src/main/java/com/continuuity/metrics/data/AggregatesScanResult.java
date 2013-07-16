package com.continuuity.metrics.data;

import com.google.common.base.Objects;

/**
 *
 */
public final class AggregatesScanResult {

  private final String context;
  private final String metric;
  private final String runId;
  private final String tag;
  private final long value;

  AggregatesScanResult(String context, String metric, String runId, String tag, long value) {
    this.context = context;
    this.metric = metric;
    this.runId = runId;
    this.tag = tag;
    this.value = value;
  }

  public String getContext() {
    return context;
  }

  public String getMetric() {
    return metric;
  }

  public String getRunId() {
    return runId;
  }

  public String getTag() {
    return tag;
  }

  public long getValue() {
    return value;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("context", context)
      .add("metric", metric)
      .add("runId", runId)
      .add("tag", tag)
      .add("value", value)
      .toString();
  }
}
