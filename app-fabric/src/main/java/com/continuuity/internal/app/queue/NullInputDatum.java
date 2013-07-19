package com.continuuity.internal.app.queue;

import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.app.queue.InputDatum;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.google.common.collect.Iterators;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * An {@link com.continuuity.app.queue.InputDatum} that has nothing inside and is not from queue.
 */
public final class NullInputDatum implements InputDatum {

  @Override
  public void submitAck(TransactionAgent txAgent) {
    // no-op
  }

  @Override
  public boolean needProcess() {
    return true;
  }

  @Override
  public Iterator<ByteBuffer> getData() {
    return Iterators.emptyIterator();
  }

  @Override
  public void incrementRetry() {
    // No-op
  }

  @Override
  public int getRetry() {
    return Integer.MAX_VALUE;
  }

  @Override
  public InputContext getInputContext() {
    return new InputContext() {
      @Override
      public String getOrigin() {
        return "";
      }

      @Override
      public int getRetryCount() {
        return Integer.MAX_VALUE;
      }
    };
  }
}
