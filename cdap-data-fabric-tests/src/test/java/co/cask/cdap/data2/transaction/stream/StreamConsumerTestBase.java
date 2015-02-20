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

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.file.FileWriter;
import co.cask.cdap.data.stream.StreamFileWriterFactory;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.queue.DequeueResult;
import co.cask.cdap.data2.queue.DequeueStrategy;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.SlowTests;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public abstract class StreamConsumerTestBase {

  private static final Comparator<StreamEvent> STREAM_EVENT_COMPARATOR = new Comparator<StreamEvent>() {
    @Override
    public int compare(StreamEvent o1, StreamEvent o2) {
      int cmp = Longs.compare(o1.getTimestamp(), o2.getTimestamp());
      if (cmp != 0) {
        return cmp;
      }
      return o1.getBody().compareTo(o2.getBody());
    }
  };

  protected abstract QueueClientFactory getQueueClientFactory();

  protected abstract StreamConsumerFactory getConsumerFactory();

  protected abstract StreamAdmin getStreamAdmin();

  protected abstract TransactionSystemClient getTransactionClient();

  protected abstract StreamFileWriterFactory getFileWriterFactory();

  @Test
  public void testFIFORollback() throws Exception {
    String stream = "testFIFORollback";
    Id.Stream streamId = Id.Stream.from(Constants.DEFAULT_NAMESPACE, stream);
    StreamAdmin streamAdmin = getStreamAdmin();
    streamAdmin.create(streamId);
    StreamConfig streamConfig = streamAdmin.getConfig(streamId);

    // Writes 5 events
    writeEvents(streamConfig, "Testing ", 5);

    streamAdmin.configureInstances(streamId, 0L, 2);

    StreamConsumerFactory consumerFactory = getConsumerFactory();
    StreamConsumer consumer0 = consumerFactory.create(streamId, "fifo.rollback",
                                                      new ConsumerConfig(0L, 0, 2, DequeueStrategy.FIFO, null));

    StreamConsumer consumer1 = consumerFactory.create(streamId, "fifo.rollback",
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

  private List<StreamEvent> writeEvents(StreamConfig streamConfig, String msgPrefix,
                                        int count, Clock clock) throws IOException {

    FileWriter<StreamEvent> writer = getFileWriterFactory().create(streamConfig, 0);
    try {
      return writeEvents(writer, msgPrefix, count, clock);
    } finally {
      writer.close();
    }
  }

  private List<StreamEvent> writeEvents(FileWriter<StreamEvent> streamWriter,
                                        String msgPrefix, int count, Clock clock) throws IOException {
    Map<String, String> headers = ImmutableMap.of();
    List<StreamEvent> result = Lists.newLinkedList();
    for (int i = 0; i < count; i++) {
      String msg = msgPrefix + i;
      StreamEvent event = new StreamEvent(headers, Charsets.UTF_8.encode(msg), clock.getTime());
      result.add(event);
      streamWriter.append(event);
    }
    return result;
  }

  private List<StreamEvent> writeEvents(StreamConfig streamConfig, String msgPrefix, int count) throws IOException {
    return writeEvents(streamConfig, msgPrefix, count, new Clock());
  }

  @Test
  public void testFIFOReconfigure() throws Exception {
    String stream = "testReconfigure";
    Id.Stream streamId = Id.Stream.from(Constants.DEFAULT_NAMESPACE, stream);
    StreamAdmin streamAdmin = getStreamAdmin();
    streamAdmin.create(streamId);
    StreamConfig streamConfig = streamAdmin.getConfig(streamId);

    // Writes 5 events
    writeEvents(streamConfig, "Testing ", 5);

    // Configure 3 consumers.
    streamAdmin.configureInstances(streamId, 0L, 3);

    StreamConsumerFactory consumerFactory = getConsumerFactory();

    // Starts three consumers
    List<StreamConsumer> consumers = Lists.newArrayList();
    for (int i = 0; i < 3; i++) {
      consumers.add(consumerFactory.create(streamId, "fifo.reconfigure",
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
    streamAdmin.configureInstances(streamId, 0L, 2);
    consumers.clear();

    for (int i = 0; i < 2; i++) {
      consumers.add(consumerFactory.create(streamId, "fifo.reconfigure",
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

  @Test
  public void testTTL() throws Exception {
    String stream = "testTTL";
    Id.Stream streamId = Id.Stream.from(Constants.DEFAULT_NAMESPACE, stream);
    StreamAdmin streamAdmin = getStreamAdmin();

    // Create stream with ttl of 1 day
    final long ttl = TimeUnit.DAYS.toMillis(1);
    final long currentTime = System.currentTimeMillis();
    final long increment = TimeUnit.SECONDS.toMillis(1);
    final long approxEarliestNonExpiredTime = currentTime - TimeUnit.HOURS.toMillis(1);

    Properties streamProperties = new Properties();
    streamProperties.setProperty(Constants.Stream.TTL, Long.toString(ttl));
    streamProperties.setProperty(Constants.Stream.PARTITION_DURATION, Long.toString(ttl));
    streamAdmin.create(streamId, streamProperties);

    StreamConfig streamConfig = streamAdmin.getConfig(streamId);
    streamAdmin.configureInstances(streamId, 0L, 1);
    StreamConsumerFactory consumerFactory = getConsumerFactory();

    Assert.assertEquals(ttl, streamConfig.getTTL());
    Assert.assertEquals(ttl, streamConfig.getPartitionDuration());

    Set<StreamEvent> expectedEvents = Sets.newTreeSet(STREAM_EVENT_COMPARATOR);
    FileWriter<StreamEvent> writer = getFileWriterFactory().create(streamConfig, 0);

    try {
      // Write 10 expired messages
      writeEvents(streamConfig, "Old event ", 20, new IncrementingClock(0, 1));
      // Write 5 non-expired messages
      expectedEvents.addAll(writeEvents(streamConfig, "New event ", 12,
                                        new IncrementingClock(approxEarliestNonExpiredTime, increment)));
    } finally {
      writer.close();
    }

    // Dequeue from stream. Should only get the 5 unexpired events.
    StreamConsumer consumer = consumerFactory.create(streamId, stream,
                                                     new ConsumerConfig(0L, 0, 1, DequeueStrategy.FIFO, null));
    try {
      verifyEvents(consumer, expectedEvents);

      TransactionContext txContext = createTxContext(consumer);
      txContext.start();
      try {
        // Should be no more pending events
        DequeueResult<StreamEvent> result = consumer.poll(1, 2, TimeUnit.SECONDS);
        Assert.assertTrue(result.isEmpty());
      } finally {
        txContext.finish();
      }
    } finally {
      consumer.close();
    }
  }

  @Test
  public void testTTLMultipleEventsWithSameTimestamp() throws Exception {
    String stream = "testTTLMultipleEventsWithSameTimestamp";
    Id.Stream streamId = Id.Stream.from(Constants.DEFAULT_NAMESPACE, stream);
    StreamAdmin streamAdmin = getStreamAdmin();

    // Create stream with ttl of 1 day
    final long ttl = TimeUnit.DAYS.toMillis(1);
    final long currentTime = System.currentTimeMillis();
    final long increment = TimeUnit.SECONDS.toMillis(1);
    final long approxEarliestNonExpiredTime = currentTime - TimeUnit.HOURS.toMillis(1);

    Properties streamProperties = new Properties();
    streamProperties.setProperty(Constants.Stream.TTL, Long.toString(ttl));
    streamProperties.setProperty(Constants.Stream.PARTITION_DURATION, Long.toString(ttl));
    streamAdmin.create(streamId, streamProperties);

    StreamConfig streamConfig = streamAdmin.getConfig(streamId);
    streamAdmin.configureInstances(streamId, 0L, 1);
    StreamConsumerFactory consumerFactory = getConsumerFactory();

    Assert.assertEquals(ttl, streamConfig.getTTL());
    Assert.assertEquals(ttl, streamConfig.getPartitionDuration());

    // Write 100 expired messages to stream with expired timestamp
    writeEvents(streamConfig, "Old event ", 10, new ConstantClock(0));

    // Write 500 non-expired messages to stream with timestamp approxEarliestNonExpiredTime..currentTime
    Set<StreamEvent> expectedEvents = Sets.newTreeSet(STREAM_EVENT_COMPARATOR);
    FileWriter<StreamEvent> writer = getFileWriterFactory().create(streamConfig, 0);

    try {
      expectedEvents.addAll(writeEvents(writer, "New event pre-flush ", 20,
                                        new IncrementingClock(approxEarliestNonExpiredTime, increment, 5)));
      writer.flush();
      expectedEvents.addAll(writeEvents(writer, "New event post-flush ", 20,
                                        new IncrementingClock(approxEarliestNonExpiredTime + 1, increment, 5)));
    } finally {
      writer.close();
    }

    StreamConsumer consumer = consumerFactory.create(streamId, stream,
                                                     new ConsumerConfig(0L, 0, 1, DequeueStrategy.FIFO, null));
    verifyEvents(consumer, expectedEvents);

    TransactionContext txContext = createTxContext(consumer);
    txContext.start();
    try {
      // Should be no more pending events
      DequeueResult<StreamEvent> result = consumer.poll(1, 1, TimeUnit.SECONDS);
      Assert.assertTrue(result.isEmpty());
    } finally {
      txContext.finish();
    }

    consumer.close();
  }

  @Category(SlowTests.class)
  @Test
  public void testTTLStartingFile() throws Exception {
    String stream = "testTTLStartingFile";
    Id.Stream streamId = Id.Stream.from(Constants.DEFAULT_NAMESPACE, stream);
    StreamAdmin streamAdmin = getStreamAdmin();

    // Create stream with ttl of 3 seconds and partition duration of 3 seconds
    final long ttl = TimeUnit.SECONDS.toMillis(3);

    Properties streamProperties = new Properties();
    streamProperties.setProperty(Constants.Stream.TTL, Long.toString(ttl));
    streamProperties.setProperty(Constants.Stream.PARTITION_DURATION, Long.toString(ttl));
    streamAdmin.create(streamId, streamProperties);

    StreamConfig streamConfig = streamAdmin.getConfig(streamId);
    streamAdmin.configureGroups(streamId, ImmutableMap.of(0L, 1, 1L, 1));
    StreamConsumerFactory consumerFactory = getConsumerFactory();

    StreamConsumer consumer = consumerFactory.create(streamId, stream,
                                                     new ConsumerConfig(0L, 0, 1, DequeueStrategy.FIFO, null));
    StreamConsumer newConsumer;
    Set<StreamEvent> expectedEvents = Sets.newTreeSet(STREAM_EVENT_COMPARATOR);

    try {
      // Create a new consumer for second consumer verification.
      // Need to create consumer before write event because in HBase, creation of consumer took couple seconds.
      newConsumer = consumerFactory.create(streamId, stream,
                                           new ConsumerConfig(1L, 0, 1, DequeueStrategy.FIFO, null));

      // write 20 events in a partition that will be expired due to sleeping the TTL
      writeEvents(streamConfig, "Phase 0 expired event ", 20);
      Thread.sleep(ttl);
      verifyEvents(consumer, expectedEvents);

      // also verify for a new consumer
      try {
        verifyEvents(newConsumer, expectedEvents);
      } finally {
        newConsumer.close();
      }

      // Create a new consumer for second consumer verification (with clean state)
      // Need to create consumer before write event because in HBase, creation of consumer took couple seconds.
      streamAdmin.configureGroups(streamId, ImmutableMap.of(0L, 1));
      streamAdmin.configureGroups(streamId, ImmutableMap.of(0L, 1, 1L, 1));
      newConsumer = consumerFactory.create(streamId, stream,
                                           new ConsumerConfig(1L, 0, 1, DequeueStrategy.FIFO, null));

      // write 20 events in a partition and read it back immediately. They shouldn't expired.
      expectedEvents.addAll(writeEvents(streamConfig, "Phase 1 non-expired event ", 20));
      verifyEvents(consumer, expectedEvents);

      // also verify for a new consumer
      try {
        verifyEvents(newConsumer, expectedEvents);
      } finally {
        newConsumer.close();
      }

      // Create a new consumer for second consumer verification (with clean state)
      // Need to create consumer before write event because in HBase, creation of consumer took couple seconds.
      streamAdmin.configureGroups(streamId, ImmutableMap.of(0L, 1));
      streamAdmin.configureGroups(streamId, ImmutableMap.of(0L, 1, 1L, 1));
      newConsumer = consumerFactory.create(streamId, stream,
                                           new ConsumerConfig(1L, 0, 1, DequeueStrategy.FIFO, null));

      // write 20 events in a partition that will be expired due to sleeping the TTL
      // This will write to a new partition different then the first batch write.
      // Also, because it sleep TTL time, the previous batch write would also get expired.
      expectedEvents.clear();
      writeEvents(streamConfig, "Phase 2 expired event ", 20);
      Thread.sleep(ttl);
      verifyEvents(consumer, expectedEvents);

      // also verify for a new consumer
      try {
        verifyEvents(newConsumer, expectedEvents);
      } finally {
        newConsumer.close();
      }


      // Create a new consumer for second consumer verification (with clean state)
      // Need to create consumer before write event because in HBase, creation of consumer took couple seconds.
      streamAdmin.configureGroups(streamId, ImmutableMap.of(0L, 1));
      streamAdmin.configureGroups(streamId, ImmutableMap.of(0L, 1, 1L, 1));
      newConsumer = consumerFactory.create(streamId, stream,
                                           new ConsumerConfig(1L, 0, 1, DequeueStrategy.FIFO, null));

      // write 20 events in a partition and read it back immediately. They shouldn't expire.
      expectedEvents.addAll(writeEvents(streamConfig, "Phase 3 non-expired event ", 20));
      verifyEvents(consumer, expectedEvents);

      // also verify for a new consumer
      try {
        verifyEvents(newConsumer, expectedEvents);
      } finally {
        newConsumer.close();
      }

      // Should be no more pending events
      expectedEvents.clear();
      verifyEvents(consumer, expectedEvents);
    } finally {
      consumer.close();
    }
  }

  private void verifyEvents(StreamConsumer consumer, Collection<StreamEvent> expectedEvents) throws Exception {
    TransactionContext txContext = createTxContext(consumer);
    txContext.start();

    try {
      Set<StreamEvent> actualEvents = Sets.newTreeSet(STREAM_EVENT_COMPARATOR);
      int size = expectedEvents.size();
      DequeueResult<StreamEvent> result = consumer.poll(size == 0 ? 1 : size, 1, TimeUnit.SECONDS);
      Iterables.addAll(actualEvents, result);
      Assert.assertEquals(expectedEvents, actualEvents);
    } finally {
      txContext.finish();
    }
  }

  private TransactionContext createTxContext(TransactionAware... txAwares) {
    return new TransactionContext(getTransactionClient(), txAwares);
  }

  private class Clock {
    public long getTime() {
      return System.currentTimeMillis();
    }
  }

  private class ConstantClock extends Clock {
    private long time;

    private ConstantClock(long time) {
      this.time = time;
    }

    @Override
    public long getTime() {
      return time;
    }
  }

  private class IncrementingClock extends Clock {
    private final int repeatsPerTimestamp;
    private final long increment;

    private int currentRepeat;
    private long current;

    public IncrementingClock(long start, long increment, int repeatsPerTimestamp) {
      Preconditions.checkArgument(repeatsPerTimestamp > 0);
      this.increment = increment;
      this.repeatsPerTimestamp = repeatsPerTimestamp;
      this.current = start;
      this.currentRepeat = 0;
    }

    public IncrementingClock(long start, long increment) {
      this(start, increment, 1);
    }

    @Override
    public long getTime() {
      final long result = current;
      if (currentRepeat % repeatsPerTimestamp == 0) {
        current += increment;
      }
      currentRepeat++;
      return result;
    }
  }
}
