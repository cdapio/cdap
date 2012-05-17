/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.overlord.metrics.client;

import com.continuuity.overlord.metrics.client.metric.AbstractMetric;

/**
 *  Lower level record interface for storing  client.
 */
public interface Record {
  /**
   * @return Timestamp the record was generated.
   */
  long getTimestamp();

  /**
   * @return Name of the record
   */
  String getName();

  /**
   * @return Description of the record
   */
  String getDescription();

  /**
   * @return Metrics associated with the record.
   */
  Iterable<AbstractMetric> getMetrics();
}
