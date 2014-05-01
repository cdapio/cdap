package com.continuuity.internal.app.queue;

import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.app.queue.InputDatum;
import com.continuuity.common.queue.QueueName;
import com.google.common.base.Objects;
import com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An {@link com.continuuity.app.queue.InputDatum} that has nothing inside and is not from queue.
 *
 * @param <T> Type of input.
 */
public final class NullInputDatum<T> implements InputDatum<T> {

  private final AtomicInteger retries = new AtomicInteger(0);
  private final InputContext inputContext =  new InputContext() {
    @Override
    public String getOrigin() {
      return "";
    }

    @Override
    public int getRetryCount() {
      return retries.get();
    }

    @Override
    public String toString() {
      return "nullInput";
    }
  };

  @Override
  public boolean needProcess() {
    return true;
  }

  @Override
  public void incrementRetry() {
    retries.incrementAndGet();
  }

  @Override
  public int getRetry() {
    return retries.get();
  }

  @Override
  public InputContext getInputContext() {
    return inputContext;
  }

  @Override
  public QueueName getQueueName() {
    return null;
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
  public Iterator<T> iterator() {
    return Iterators.emptyIterator();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("retries", retries.get())
      .toString();
  }
}
