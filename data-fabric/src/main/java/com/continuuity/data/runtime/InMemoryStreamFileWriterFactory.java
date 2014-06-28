/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.runtime;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.queue.QueueName;
import com.continuuity.common.stream.StreamEventCodec;
import com.continuuity.data.file.FileWriter;
import com.continuuity.data.stream.StreamFileWriterFactory;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.queue.QueueEntry;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.TransactionFailureException;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;

/**
 * A Mock {@link StreamFileWriterFactory} that actually doesn't write to file, but to in memory queue instead.
 */
final class InMemoryStreamFileWriterFactory implements StreamFileWriterFactory {

  private static final StreamEventCodec STREAM_EVENT_CODEC = new StreamEventCodec();

  private final QueueClientFactory queueClientFactory;
  private final TransactionExecutorFactory executorFactory;

  @Inject
  InMemoryStreamFileWriterFactory(QueueClientFactory queueClientFactory, TransactionExecutorFactory executorFactory) {
    this.queueClientFactory = queueClientFactory;
    this.executorFactory = executorFactory;
  }

  @Override
  public FileWriter<StreamEvent> create(StreamConfig config, int generation) throws IOException {
    final Queue2Producer producer = queueClientFactory.createProducer(QueueName.fromStream(config.getName()));
    final List<TransactionAware> txAwares = Lists.newArrayList();
    if (producer instanceof TransactionAware) {
      txAwares.add((TransactionAware) producer);
    }
    final TransactionExecutor txExecutor = executorFactory.createExecutor(txAwares);

    // Adapt the FileWriter interface into Queue2Producer
    return new FileWriter<StreamEvent>() {

      private final List<StreamEvent> events = Lists.newArrayList();

      @Override
      public void append(StreamEvent event) throws IOException {
        events.add(event);
      }

      @Override
      public void close() throws IOException {
        if (producer instanceof Closeable) {
          ((Closeable) producer).close();
        }
      }

      @Override
      public void flush() throws IOException {
        try {
          txExecutor.execute(new TransactionExecutor.Subroutine() {
            @Override
            public void apply() throws Exception {
              for (StreamEvent event : events) {
                producer.enqueue(new QueueEntry(STREAM_EVENT_CODEC.encodePayload(event)));
              }
              events.clear();
            }
          });
        } catch (TransactionFailureException e) {
          throw new IOException(e);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new InterruptedIOException();
        }
      }
    };
  }
}
