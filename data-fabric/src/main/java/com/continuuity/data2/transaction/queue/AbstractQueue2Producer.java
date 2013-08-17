/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.transaction.TransactionAware;

/**
 * Abstract base class for {@link Queue2Producer} that emits enqueue metrics post commit.
 */
public abstract class AbstractQueue2Producer implements Queue2Producer, TransactionAware {

  private final QueueMetrics queueMetrics;

  protected AbstractQueue2Producer(QueueMetrics queueMetrics) {
    this.queueMetrics = queueMetrics;
  }

  @Override
  public void postTxCommit() {
    int count = getLastEnqueueCount();
    if (count > 0) {
      queueMetrics.emitEnqueue(count);
    }
  }

  protected abstract int getLastEnqueueCount();
}
