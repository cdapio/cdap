package com.continuuity.internal.app.queue;

import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.app.queue.InputDatum;

import java.nio.ByteBuffer;

/**
 * An {@link com.continuuity.app.queue.InputDatum} that has nothing inside and is not from queue.
 */
public final class NullInputDatum implements InputDatum {

  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

  @Override
  public void submitAck(TransactionAgent txAgent) {
    // no-op
  }

  @Override
  public boolean needProcess() {
    return true;
  }

  @Override
  public ByteBuffer getData() {
    return EMPTY_BUFFER;
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
      public String getName() {
        return "";
      }

      @Override
      public int getRetryCount() {
        return Integer.MAX_VALUE;
      }
    };
  }
}
