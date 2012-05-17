/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.overlord.metrics.client;

import com.continuuity.overlord.metrics.client.metric.AbstractMetric;

/**
 * Interface for helping build {@link Record}
 */
public interface RecordBuilder {
  /**
   * Adds a metric to {@link RecordBuilder}
   * @param metric  Metric to be added
   * @return {@link RecordBuilder}
   */
  public abstract RecordBuilder addMetric(AbstractMetric metric);

  /**
   * Adds a counters as metric to {@link RecordBuilder}
   * @param info Information about metric {@link Info}
   * @param value value of the metric.
   * @return
   */
  public abstract RecordBuilder addCounter(Info info, int value);

  /**
   * @return Gets a record
   */
  public Record getRecord();
}
