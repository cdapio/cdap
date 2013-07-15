/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.collect;

import com.continuuity.metrics.transport.MetricsRecord;

/**
 * A MetricsEmitter is a class that is able to emit {@link com.continuuity.metrics.transport.MetricsRecord}.
 */
public interface MetricsEmitter {

  /**
   * Emits metric for the given timestamp.
   * @param timestamp The timestamp for the metrics.
   * @return A {@link com.continuuity.metrics.transport.MetricsRecord} representing metrics for the given timestamp.
   */
  MetricsRecord emit(long timestamp);
}
