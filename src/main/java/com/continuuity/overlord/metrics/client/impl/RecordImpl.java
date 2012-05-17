/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.overlord.metrics.client.impl;

import com.continuuity.overlord.metrics.client.Info;
import com.continuuity.overlord.metrics.client.Record;
import com.continuuity.overlord.metrics.client.metric.AbstractMetric;
import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class RecordImpl implements Record {
  private static final Logger LOG = LoggerFactory.getLogger(RecordImpl.class);

  /** Timestamp the record was generated */
  private final long timestamp;

  /** Name/Description of the record */
  private final Info info;

  /** Collection of client that are part of the record. */
  private final Iterable<AbstractMetric> metrics;

  /**
   *
   * @param timestamp
   * @param info
   * @param metrics
   */
  public RecordImpl(long timestamp, Info info,  Iterable<AbstractMetric> metrics) {
    this.timestamp = timestamp;
    this.info = info;
    this.metrics = metrics;
  }

  /**
   * @return Timestamp the metric was snapshotted.
   */
  @Override
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * @return Name of record
   */
  @Override
  public String getName() {
    return info.getName();
  }

  /**
   * @return Description for record.
   */
  @Override
  public String getDescription() {
    return info.getDescription();
  }

  /**
   * @return Collection of client that are part of this record.
   */
  @Override
  public Iterable<AbstractMetric> getMetrics() {
    return metrics;
  }

  /**
   * @return  String representation of Record
   */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("timestamp", timestamp)
      .add("info", info)
      .add("metric", metrics)
      .toString();
  }
}
