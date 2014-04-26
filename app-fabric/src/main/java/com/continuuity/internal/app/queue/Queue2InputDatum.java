/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.queue;

import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.app.queue.InputDatum;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.DequeueResult;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Iterators;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An abstract implementation for {@link InputDatum} for common operations across old and new queue.
 *
 * @param <T> Type of input.
 *
 * TODO: This class should be renamed to QueueInputDatum once migration to txds2 is completed.
 */
final class Queue2InputDatum<T> implements InputDatum<T> {

  private static final Function<byte[], ByteBuffer> BYTE_ARRAY_TO_BYTE_BUFFER = new Function<byte[], ByteBuffer>() {
    @Override
    public ByteBuffer apply(byte[] input) {
      return ByteBuffer.wrap(input);
    }
  };

  private final DequeueResult<byte[]> result;
  private final AtomicInteger retry;
  private final InputContext inputContext;
  private final QueueName queueName;
  private final Function<ByteBuffer, T> decoder;

  Queue2InputDatum(final QueueName queueName, DequeueResult<byte[]> result, Function<ByteBuffer, T> decoder) {
    this.result = result;
    this.retry = new AtomicInteger(0);
    this.queueName = queueName;
    this.decoder = decoder;
    this.inputContext = new InputContext() {
      @Override
      public String getOrigin() {
        return queueName.getSimpleName();
      }

      @Override
      public int getRetryCount() {
        return retry.get();
      }

      @Override
      public String toString() {
        return Objects.toStringHelper(InputContext.class)
          .add("queue", queueName)
          .toString();
      }
    };
  }

  @Override
  public boolean needProcess() {
    return !result.isEmpty();
  }

  @Override
  public Iterator<T> iterator() {
    return Iterators.transform(Iterators.transform(result.iterator(), BYTE_ARRAY_TO_BYTE_BUFFER), decoder);
  }

  @Override
  public void incrementRetry() {
    retry.incrementAndGet();
  }

  @Override
  public int getRetry() {
    return retry.get();
  }

  @Override
  public InputContext getInputContext() {
    return inputContext;
  }

  @Override
  public QueueName getQueueName() {
    return queueName;
  }

  @Override
  public void reclaim() {
    result.reclaim();
  }

  @Override
  public int size() {
    return result.size();
  }

  @Override
  public String toString() {
    return String.format("%s %d", result, retry.get());
  }
}
