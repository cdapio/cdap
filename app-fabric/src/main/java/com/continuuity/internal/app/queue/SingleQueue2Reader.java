/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.queue;

import com.continuuity.data2.OperationException;
import com.continuuity.app.queue.InputDatum;
import com.continuuity.app.queue.QueueReader;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data2.queue.Queue2Consumer;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import java.io.IOException;

/**
 * A {@link QueueReader} for reading from {@link Queue2Consumer}.
 */
public final class SingleQueue2Reader implements QueueReader {

  private final Supplier<Queue2Consumer> consumerSupplier;
  private final int batchSize;

  @Inject
  SingleQueue2Reader(@Assisted Supplier<Queue2Consumer> consumerSupplier,
                     @Assisted int batchSize) {
    this.consumerSupplier = consumerSupplier;
    this.batchSize = batchSize;
  }

  @Override
  public InputDatum dequeue() throws OperationException {
    Queue2Consumer consumer = consumerSupplier.get();
    try {
      return new Queue2InputDatum(consumer.getQueueName(), consumer.dequeue(batchSize));
    } catch (IOException e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage(), e);
    }
  }
}
