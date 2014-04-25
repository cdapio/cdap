/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.transaction.TransactionAware;
import com.google.common.collect.Iterators;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
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
  String getStreamName();

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
   * @return An instance of {@link Result}
   * @throws IOException If there is error while reading events
   * @throws InterruptedException If interrupted while waiting
   */
  Result poll(int maxEvents, long timeout, TimeUnit timeoutUnit) throws IOException, InterruptedException;

  Result EMPTY_RESULT = new Result() {
    @Override
    public boolean isEmpty() {
      return true;
    }

    @Override
    public int size() {
      return 0;
    }

    @Override
    public Iterator<StreamEvent> iterator() {
      return Iterators.emptyIterator();
    }
  };

  /**
   * Represents the result of poll.
   */
  interface Result extends Iterable<StreamEvent> {

    /**
     * @return true if it has no stream event.
     */
    boolean isEmpty();

    /**
     * @return Number of stream events in this result.
     */
    int size();
  }
}
