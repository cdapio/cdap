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
 * TODO: This class should be renamed to QueueInputDatum once migration to txds2 is completed.
 */
public final class Queue2InputDatum implements InputDatum {

  private static final Function<byte[], ByteBuffer> BYTE_ARRAY_TO_BYTE_BUFFER = new Function<byte[], ByteBuffer>() {
    @Override
    public ByteBuffer apply(byte[] input) {
      return ByteBuffer.wrap(input);
    }
  };

  private final DequeueResult result;
  private final AtomicInteger retry;
  private final InputContext inputContext;

  public Queue2InputDatum(final QueueName queueName, final DequeueResult result) {
    this.result = result;
    this.retry = new AtomicInteger(0);
    this.inputContext = new InputContext() {
      @Override
      public String getOrigin() {
        return queueName.getSimpleName();
      }

      @Override
      public QueueName getOriginQueueName() {
        return queueName;
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
  public Iterator<ByteBuffer> iterator() {
    return Iterators.transform(result.iterator(), BYTE_ARRAY_TO_BYTE_BUFFER);
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
