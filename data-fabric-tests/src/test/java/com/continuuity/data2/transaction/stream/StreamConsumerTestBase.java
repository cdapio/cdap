/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.queue.QueueName;
import com.continuuity.common.stream.DefaultStreamEvent;
import com.continuuity.data.file.FileWriter;
import com.continuuity.data.stream.TimePartitionedStreamFileWriter;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.DequeueResult;
import com.continuuity.data2.queue.DequeueStrategy;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionContext;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public abstract class StreamConsumerTestBase {

  protected abstract StreamConsumerFactory getConsumerFactory();

  protected abstract StreamAdmin getStreamAdmin();

  protected abstract TransactionSystemClient getTransactionClient();

  protected abstract String getStreamFilePrefix();

  @Test
  public void testFIFORollback() throws Exception {
    String stream = "testFIFORollback";
    QueueName streamName = QueueName.fromStream(stream);
    StreamAdmin streamAdmin = getStreamAdmin();
    streamAdmin.create(stream);
    StreamConfig streamConfig = streamAdmin.getConfig(stream);

    // Writes 5 events
    writeEvents(streamConfig, "Testing ", 5);

    streamAdmin.configureInstances(streamName, 0L, 2);

    StreamConsumerFactory consumerFactory = getConsumerFactory();
    StreamConsumer consumer0 = consumerFactory.create(streamName, "fifo.rollback",
                                                      new ConsumerConfig(0L, 0, 2, DequeueStrategy.FIFO, null));

    StreamConsumer consumer1 = consumerFactory.create(streamName, "fifo.rollback",
                                                      new ConsumerConfig(0L, 1, 2, DequeueStrategy.FIFO, null));

    // Try to dequeue using both consumers
    TransactionContext context0 = createTxContext(consumer0);
    TransactionContext context1 = createTxContext(consumer1);

    context0.start();
    context1.start();

    DequeueResult<StreamEvent> result0 = consumer0.poll(1, 1, TimeUnit.SECONDS);
    DequeueResult<StreamEvent> result1 = consumer1.poll(1, 1, TimeUnit.SECONDS);

    Assert.assertEquals("Testing 0", Charsets.UTF_8.decode(result0.iterator().next().getBody()).toString());
    Assert.assertEquals("Testing 1", Charsets.UTF_8.decode(result1.iterator().next().getBody()).toString());

    // Commit the first one, rollback the second one.
    context0.finish();
    context1.abort();

    // Dequeue again with the consuemrs
    context0.start();
    context1.start();

    result0 = consumer0.poll(1, 1, TimeUnit.SECONDS);
    result1 = consumer1.poll(1, 1, TimeUnit.SECONDS);

    // Expect consumer 0 keep proceeding while consumer 1 will retry with what it claimed in previous transaction.
    // This is the optimization in FIFO mode to avoid going back and rescanning.
    Assert.assertEquals("Testing 2", Charsets.UTF_8.decode(result0.iterator().next().getBody()).toString());
    Assert.assertEquals("Testing 1", Charsets.UTF_8.decode(result1.iterator().next().getBody()).toString());

    // Commit both
    context0.finish();
    context1.finish();

    consumer0.close();
    consumer1.close();
  }

  private void writeEvents(StreamConfig streamConfig, String msgPrefix, int count) throws IOException {
    Map<String, String> headers = ImmutableMap.of();
    FileWriter<StreamEvent> writer = new TimePartitionedStreamFileWriter(streamConfig, getStreamFilePrefix());
    for (int i = 0; i < count; i++) {
      String msg = msgPrefix + i;
      writer.append(new DefaultStreamEvent(headers, Charsets.UTF_8.encode(msg), System.currentTimeMillis()));
    }
    writer.close();
  }

  @Test
  public void testFIFOReconfigure() throws Exception {
    String stream = "testReconfigure";
    QueueName streamName = QueueName.fromStream(stream);
    StreamAdmin streamAdmin = getStreamAdmin();
    streamAdmin.create(stream);
    StreamConfig streamConfig = streamAdmin.getConfig(stream);

    // Writes 5 events
    writeEvents(streamConfig, "Testing ", 5);

    // Configure 3 consumers.
    streamAdmin.configureInstances(streamName, 0L, 3);

    StreamConsumerFactory consumerFactory = getConsumerFactory();

    // Starts three consumers
    List<StreamConsumer> consumers = Lists.newArrayList();
    for (int i = 0; i < 3; i++) {
      consumers.add(consumerFactory.create(streamName, "fifo.reconfigure",
                                           new ConsumerConfig(0L, i, 3, DequeueStrategy.FIFO, null)));
    }

    List<TransactionContext> txContexts = Lists.newArrayList();
    for (StreamConsumer consumer : consumers) {
      txContexts.add(createTxContext(consumer));
    }

    for (TransactionContext txContext : txContexts) {
      txContext.start();
    }

    // Consumer an item from each consumer, but only have the first one commit.
    for (int i = 0; i < consumers.size(); i++) {
      DequeueResult<StreamEvent> result = consumers.get(i).poll(1, 1, TimeUnit.SECONDS);
      Assert.assertEquals("Testing " + i, Charsets.UTF_8.decode(result.iterator().next().getBody()).toString());

      if (i == 0) {
        txContexts.get(i).finish();
      } else {
        txContexts.get(i).abort();
      }
    }

    for (StreamConsumer consumer : consumers) {
      consumer.close();
    }

    // Reconfigure to have two consumers.
    streamAdmin.configureInstances(streamName, 0L, 2);
    consumers.clear();

    for (int i = 0; i < 2; i++) {
      consumers.add(consumerFactory.create(streamName, "fifo.reconfigure",
                                           new ConsumerConfig(0L, i, 2, DequeueStrategy.FIFO, null)));
    }

    txContexts.clear();
    for (StreamConsumer consumer : consumers) {
      txContexts.add(createTxContext(consumer));
    }

    // Consumer an item from each consumer, they should see all four items.
    Set<String> messages = Sets.newTreeSet();
    boolean done;
    do {
      for (TransactionContext txContext : txContexts) {
        txContext.start();
      }

      done = true;
      for (int i = 0; i < consumers.size(); i++) {
        DequeueResult<StreamEvent> result = consumers.get(i).poll(1, 1, TimeUnit.SECONDS);
        if (result.isEmpty()) {
          continue;
        }
        done = false;
        messages.add(Charsets.UTF_8.decode(result.iterator().next().getBody()).toString());
        txContexts.get(i).finish();
      }
    } while (!done);

    Assert.assertEquals(4, messages.size());
    int count = 1;
    for (String msg : messages) {
      Assert.assertEquals("Testing " + count, msg);
      count++;
    }

    for (StreamConsumer consumer : consumers) {
      consumer.close();
    }
  }

  private TransactionContext createTxContext(TransactionAware... txAwares) {
    return new TransactionContext(getTransactionClient(), txAwares);
  }
}
