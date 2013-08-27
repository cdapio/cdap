package com.continuuity.internal.app.queue;

import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.app.queue.InputDatum;
import com.google.common.collect.Iterators;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * An {@link com.continuuity.app.queue.InputDatum} that has nothing inside and is not from queue.
 */
public final class NullInputDatum implements InputDatum {

  @Override
  public boolean needProcess() {
    return true;
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

      @Override
      public String toString() {
        return "nullInput";
      }
    };
  }

  @Override
  public void reclaim() {
    // No-op
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public Iterator<ByteBuffer> iterator() {
    return Iterators.emptyIterator();
  }
}
