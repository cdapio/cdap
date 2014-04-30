/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.DequeueResult;
import com.continuuity.data2.transaction.TransactionAware;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Represents a transaction stream consumer. On each call to {@link #poll(int, long, java.util.concurrent.TimeUnit)},
 * it will try ro read from stream starting from where it get initialized with or from what last poll left off.
 *
 * On transaction rollback, all events that are read by poll calls will be reverted so that when poll is issued on a
 * new transaction afterwards, it will start giving stream events from the where last committed poll ended.
 */
public interface StreamConsumer extends Closeable, TransactionAware {

  /**
   * @return Name of the stream this consumer is consuming.
   */
  QueueName getStreamName();

  /**
   * @return Configuration of this consumer.
   */
  ConsumerConfig getConsumerConfig();

  /**
   * Retrieves up to {@code maxEvents} of {@link StreamEvent} from the stream.
   *
   * @param maxEvents Maximum number of events to retrieve
   * @param timeout Maximum of time to spend on trying to read up to maxEvents
   * @param timeoutUnit Unit for the timeout
   *
   * @return An instance of {@link DequeueResult} which carries {@link StreamEvent}s inside.
   *
   * @throws IOException If there is error while reading events
   * @throws InterruptedException If interrupted while waiting
   */
  DequeueResult<StreamEvent> poll(int maxEvents,
                                  long timeout, TimeUnit timeoutUnit) throws IOException, InterruptedException;
}
