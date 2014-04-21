/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.queue;

import com.continuuity.app.queue.InputDatum;
import com.continuuity.app.queue.QueueReader;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data2.OperationException;
import com.continuuity.data2.queue.Queue2Consumer;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@link QueueReader} for reading from {@link Queue2Consumer}.
 *
 * @param <T> Type of input dequeued from this reader.
 */
public final class SingleQueue2Reader<T> implements QueueReader<T> {

  private final Supplier<Queue2Consumer> consumerSupplier;
  private final int batchSize;
  private final Function<ByteBuffer, T> decoder;

  @Inject
  SingleQueue2Reader(@Assisted Supplier<Queue2Consumer> consumerSupplier,
                     @Assisted int batchSize,
                     @Assisted Function<ByteBuffer, T> decoder) {
    this.consumerSupplier = consumerSupplier;
    this.batchSize = batchSize;
    this.decoder = decoder;
  }

  @Override
  public InputDatum<T> dequeue() throws OperationException {
    Queue2Consumer consumer = consumerSupplier.get();
    try {
      return new Queue2InputDatum<T>(consumer.getQueueName(), consumer.dequeue(batchSize), decoder);
    } catch (IOException e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage(), e);
    }
  }
}
