/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.queue;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.app.queue.InputDatum;
import com.continuuity.app.queue.QueueReader;
import com.continuuity.data2.transaction.stream.StreamConsumer;
import com.google.common.base.Function;
import com.google.common.base.Supplier;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * A {@link QueueReader} implementation that dequeue from stream
 *
 * @param <T> Target type.
 */
public final class StreamQueueReader<T> implements QueueReader<T> {

  private final Supplier<StreamConsumer> consumerSupplier;
  private final int batchSize;
  private final Function<StreamEvent, T> eventTransform;

  StreamQueueReader(Supplier<StreamConsumer> consumerSupplier, int batchSize,
                    Function<StreamEvent, T> eventTransform) {
    this.consumerSupplier = consumerSupplier;
    this.batchSize = batchSize;
    this.eventTransform = eventTransform;
  }

  @Override
  public InputDatum<T> dequeue(long timeout, TimeUnit timeoutUnit) throws IOException, InterruptedException {
    StreamConsumer consumer = consumerSupplier.get();
    return new BasicInputDatum<StreamEvent, T>(consumer.getStreamName(),
                                               consumer.poll(batchSize, timeout, timeoutUnit), eventTransform);
  }
}
