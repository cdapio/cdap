/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.collect;

import com.continuuity.metrics.transport.MetricRecord;

/**
 * A MetricsEmitter is a class that is able to emit {@link MetricRecord}.
 */
public interface MetricsEmitter {

  /**
   * Emits metric for the given timestamp.
   * @param timestamp The timestamp for the metrics.
   * @return A {@link MetricRecord} representing metrics for the given timestamp.
   */
  MetricRecord emit(long timestamp);
}
