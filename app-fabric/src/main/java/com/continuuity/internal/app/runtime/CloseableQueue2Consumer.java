/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime;

import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data2.queue.ForwardingQueue2Consumer;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.transaction.TransactionAware;

import java.io.Closeable;
import java.io.IOException;

/**
 * A {@link TransactionAware} {@link Queue2Consumer} that removes itself from dataset context when closed.
 * All queue operations are forwarded to another {@link Queue2Consumer}.
 */
final class CloseableQueue2Consumer extends ForwardingQueue2Consumer implements Closeable {

  private final DataSetInstantiator context;

  CloseableQueue2Consumer(DataSetInstantiator context, Queue2Consumer consumer) {
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
