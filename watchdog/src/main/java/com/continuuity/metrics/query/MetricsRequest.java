/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import java.net.URI;

/**
 * Representing a metric query request.
 */
interface MetricsRequest {

  /**
   * Type of the request.
   */
  enum Type {
    TIME_SERIES,
    SUMMARY,
    AGGREGATE
  }

  URI getRequestURI();

  String getContextPrefix();

  String getRunId();

  String getMetricPrefix();

  String getTagPrefix();

  long getStartTime();

  long getEndTime();

  Type getType();

  int getCount();
}
