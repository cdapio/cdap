/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

/**
 *
 */
public interface QueueMetrics {

  void emitEnqueue(int count);

  void emitEnqueueBytes(int bytes);

  static final QueueMetrics NOOP_QUEUE_METRICS = new QueueMetrics() {
    @Override
    public void emitEnqueue(int count) {
      // no-op
    }

    @Override
    public void emitEnqueueBytes(int bytes) {
      // no-op
    }
  };
}
