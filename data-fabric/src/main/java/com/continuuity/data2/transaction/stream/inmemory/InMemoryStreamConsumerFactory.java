/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream.inmemory;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.queue.QueueName;
import com.continuuity.common.stream.StreamEventCodec;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.DequeueResult;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.stream.StreamConsumer;
import com.continuuity.data2.transaction.stream.StreamConsumerFactory;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * In memory implementation of StreamConsumer would be using the in memory queue implementation.
 */
public final class InMemoryStreamConsumerFactory implements StreamConsumerFactory {

  private static final StreamEventCodec STREAM_EVENT_CODEC = new StreamEventCodec();
  private final QueueClientFactory queueClientFactory;

  @Inject
  public InMemoryStreamConsumerFactory(QueueClientFactory queueClientFactory) {
    this.queueClientFactory = queueClientFactory;
  }

  @Override
  public StreamConsumer create(final QueueName streamName, String namespace,
                               final ConsumerConfig consumerConfig) throws IOException {

    final Queue2Consumer consumer = queueClientFactory.createConsumer(streamName, consumerConfig, -1);

    // An adapter from Queue2Consumer to StreamConsumer
    return new StreamConsumer() {
      @Override
      public QueueName getStreamName() {
        return streamName;
      }

      @Override
      public ConsumerConfig getConsumerConfig() {
        return consumerConfig;
      }

      @Override
      public DequeueResult<StreamEvent> poll(int maxEvents, long timeout,
                                             TimeUnit timeoutUnit) throws IOException, InterruptedException {
        final DequeueResult<byte[]> result = consumer.dequeue(maxEvents);

        // Decode byte array into stream event
        ImmutableList.Builder<StreamEvent> builder = ImmutableList.builder();
        for (byte[] content : result) {
          builder.add(STREAM_EVENT_CODEC.decodePayload(content));
        }
        final List<StreamEvent> events = builder.build();

        return new DequeueResult<StreamEvent>() {
          @Override
          public boolean isEmpty() {
            return result.isEmpty();
          }

          @Override
          public void reclaim() {

          }

          @Override
          public int size() {
            return 0;
          }

          @Override
          public Iterator<StreamEvent> iterator() {
            return events.iterator();
          }
        };
      }

      @Override
      public void close() throws IOException {
        if (consumer instanceof Closeable) {
          ((Closeable) consumer).close();
        }
      }

      @Override
      public void startTx(Transaction tx) {
        if (consumer instanceof TransactionAware) {
          ((TransactionAware) consumer).startTx(tx);
        }
      }

      @Override
      public Collection<byte[]> getTxChanges() {
        if (consumer instanceof TransactionAware) {
          return ((TransactionAware) consumer).getTxChanges();
        }
        return ImmutableList.of();
      }

      @Override
      public boolean commitTx() throws Exception {
        if (consumer instanceof TransactionAware) {
          return ((TransactionAware) consumer).commitTx();
        }
        return true;
      }

      @Override
      public void postTxCommit() {
        if (consumer instanceof TransactionAware) {
          ((TransactionAware) consumer).postTxCommit();
        }
      }

      @Override
      public boolean rollbackTx() throws Exception {
        if (consumer instanceof TransactionAware) {
          return ((TransactionAware) consumer).rollbackTx();
        }
        return true;
      }

      @Override
      public String getName() {
        return Objects.toStringHelper(this)
          .add("queue", streamName)
          .toString();
      }
    };
  }
}
