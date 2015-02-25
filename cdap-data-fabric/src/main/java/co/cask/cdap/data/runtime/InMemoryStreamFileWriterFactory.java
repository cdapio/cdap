/*
 * Copyright © 2014-2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.data.runtime;

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.common.stream.StreamEventCodec;
import co.cask.cdap.data.file.FileWriter;
import co.cask.cdap.data.stream.StreamFileWriterFactory;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.queue.QueueEntry;
import co.cask.cdap.data2.queue.QueueProducer;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionFailureException;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Iterator;
import java.util.List;

/**
 * A Mock {@link StreamFileWriterFactory} that actually doesn't write to file, but to in memory queue instead.
 */
public final class InMemoryStreamFileWriterFactory implements StreamFileWriterFactory {

  private static final StreamEventCodec STREAM_EVENT_CODEC = new StreamEventCodec();

  private final QueueClientFactory queueClientFactory;
  private final TransactionExecutorFactory executorFactory;

  @Inject
  InMemoryStreamFileWriterFactory(QueueClientFactory queueClientFactory, TransactionExecutorFactory executorFactory) {
    this.queueClientFactory = queueClientFactory;
    this.executorFactory = executorFactory;
  }

  @Override
  public String getFileNamePrefix() {
    // There is no file being created
    return "";
  }

  @Override
  public FileWriter<StreamEvent> create(StreamConfig config, int generation) throws IOException {
    final QueueProducer producer = queueClientFactory.createProducer(QueueName.fromStream(config.getStreamId()));
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
      public void appendAll(Iterator<? extends StreamEvent> events) throws IOException {
        Iterators.addAll(this.events, events);
      }

      @Override
      public void close() throws IOException {
        producer.close();
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
