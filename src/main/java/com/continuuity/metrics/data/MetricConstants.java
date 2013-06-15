package com.continuuity.metrics.data;

import com.continuuity.api.common.Bytes;

/**
 * Define constants for byte[] that are needed in multiple classes.
 */
final class MetricConstants {

  static final byte[] CONTEXT = Bytes.toBytes("context");
  static final byte[] METRIC = Bytes.toBytes("metric");

  static final String EMPTY_TAG = "-";
  static final int DEFAULT_CONTEXT_DEPTH = 5;
  static final int DEFAULT_METRIC_DEPTH = 3;
  static final int DEFAULT_TAG_DEPTH = 3;


  private MetricConstants() {
  }
}
