/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.transport;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;

/**
 * Record class for carrying information about one metric.
 */
@Nonnull
public final class MetricRecord {

  private final String context;             // Program context of where the metric get generated.
  private final String name;                // Name of the metric
  private final List<TagMetric> tags;        // List of TagMetric
  private final long timestamp;             // Timestamp in second of when the metric happened.
  private final int value;                  // Value of the metric, regardless of tags

  public MetricRecord(String context, String name, Iterable<TagMetric> tags, long timestamp, int value) {
    this.context = context;
    this.timestamp = timestamp;
    Preconditions.checkNotNull(name, "Name cannot be null.");
    this.name = name;
    this.value = value;
    this.tags = ImmutableList.copyOf(tags);
  }

  public String getContext() {
    return context;
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
}
