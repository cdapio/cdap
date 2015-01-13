/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.data2.transaction.stream;

import co.cask.cdap.api.data.schema.SchemaHash;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.common.io.BinaryDecoder;
import co.cask.cdap.common.io.Decoder;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.common.stream.StreamEventCodec;
import co.cask.cdap.common.stream.StreamEventDataCodec;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.queue.DequeueResult;
import co.cask.cdap.data2.queue.QueueConsumer;
import co.cask.common.io.ByteBufferInputStream;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * An adapter from Queue2Consumer to StreamConsumer.
 */
public final class QueueToStreamConsumer implements StreamConsumer {

  private static final StreamEventCodec STREAM_EVENT_CODEC = new StreamEventCodec();

  private final QueueName streamName;
  private final ConsumerConfig consumerConfig;
  private final QueueConsumer consumer;

  public QueueToStreamConsumer(QueueName streamName, ConsumerConfig consumerConfig, QueueConsumer consumer) {
    this.streamName = streamName;
    this.consumerConfig = consumerConfig;
    this.consumer = consumer;
  }

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
      try {
        builder.add(STREAM_EVENT_CODEC.decodePayload(content));
      } catch (Throwable t) {
        // If failed to decode, it maybe using old (pre 2.1) stream codec. Try to decode with old one.
        ByteBuffer buffer = ByteBuffer.wrap(content);
        SchemaHash schemaHash = new SchemaHash(buffer);
        Preconditions.checkArgument(schemaHash.equals(StreamEventDataCodec.STREAM_DATA_SCHEMA.getSchemaHash()),
                                    "Schema from payload not matching with StreamEventData schema.");

        Decoder decoder = new BinaryDecoder(new ByteBufferInputStream(buffer));
        // In old schema, timestamp is not recorded.
        builder.add(new StreamEvent(StreamEventDataCodec.decode(decoder), 0));
      }
    }
    final List<StreamEvent> events = builder.build();

    return new DequeueResult<StreamEvent>() {
      @Override
      public boolean isEmpty() {
        return events.isEmpty();
      }

      @Override
      public void reclaim() {
        result.reclaim();
      }

      @Override
      public int size() {
        return events.size();
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
  public String getTransactionAwareName() {
    return Objects.toStringHelper(this)
      .add("queue", streamName)
      .add("config", consumerConfig)
      .toString();
  }
}
