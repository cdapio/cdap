/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime;

import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data2.queue.ForwardingQueueConsumer;
import com.continuuity.data2.queue.QueueConsumer;
import com.continuuity.data2.transaction.TransactionAware;

import java.io.Closeable;
import java.io.IOException;

/**
 * A {@link TransactionAware} {@link QueueConsumer} that removes itself from dataset context when closed.
 * All queue operations are forwarded to another {@link QueueConsumer}.
 */
final class CloseableQueueConsumer extends ForwardingQueueConsumer implements Closeable {

  private final DataSetInstantiator context;

  CloseableQueueConsumer(DataSetInstantiator context, QueueConsumer consumer) {
    super(consumer);
    this.context = context;
  }

  @Override
  public void close() throws IOException {
    try {
      if (consumer instanceof Closeable) {
        ((Closeable) consumer).close();
      }
    } finally {
      context.removeTransactionAware(this);
    }
  }
}
