/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.queue;

import com.continuuity.app.queue.InputDatum;
import com.continuuity.app.queue.QueueReader;
import com.continuuity.data2.queue.Queue2Consumer;
import com.google.common.base.Function;
import com.google.common.base.Supplier;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * A {@link QueueReader} for reading from {@link Queue2Consumer}.
 *
 * @param <T> Type of input dequeued from this reader.
 */

public final class SingleQueue2Reader<T> extends TimeTrackingQueueReader<T> {

  private final Supplier<Queue2Consumer> consumerSupplier;
  private final int batchSize;
  private final Function<byte[], T> decoder;

  SingleQueue2Reader(Supplier<Queue2Consumer> consumerSupplier, int batchSize, final Function<ByteBuffer, T> decoder) {
    this.consumerSupplier = consumerSupplier;
    this.batchSize = batchSize;
    this.decoder = new Function<byte[], T>() {
      @Override
      public T apply(byte[] input) {
        return decoder.apply(ByteBuffer.wrap(input));
      }
    };
  }

  @Override
  public InputDatum<T> tryDequeue(long timeout, TimeUnit timeoutUnit) throws IOException {
    Queue2Consumer consumer = consumerSupplier.get();
    return new BasicInputDatum<byte[], T>(consumer.getQueueName(), consumer.dequeue(batchSize), decoder);
  }
}
