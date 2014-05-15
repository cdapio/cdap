/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.transport;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Record class for carrying information about one metric.
 */
@Nonnull
public final class MetricsRecord {

  private final String context;             // Program context of where the metric get generated.
  private final String runId;               // RunId
  private final String name;                // Name of the metric
  private final List<TagMetric> tags;       // List of TagMetric
  private final long timestamp;             // Timestamp in second of when the metric happened.
  private final int value;                  // Value of the metric, regardless of tags

  public MetricsRecord(String context, String runId, String name, Iterable<TagMetric> tags, long timestamp, int value) {
    this.context = context;
    this.runId = runId;
    this.timestamp = timestamp;
    this.name = name;
    this.value = value;
    this.tags = ImmutableList.copyOf(tags);
  }

  public String getContext() {
    return context;
  }

  public String getRunId() {
    return runId;
  }

  public String getName() {
    return name;
  }

  public Collection<TagMetric> getTags() {
    return tags;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public int getValue() {
    return value;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(MetricsRecord.class)
      .add("context", context)
      .add("runId", runId)
      .add("name", name)
      .add("tags", tags)
      .add("timestamp", timestamp)
      .add("value", value)
      .toString();
  }
}
