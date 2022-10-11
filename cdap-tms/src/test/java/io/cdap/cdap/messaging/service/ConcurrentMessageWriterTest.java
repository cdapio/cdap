/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.messaging.service;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.Uninterruptibles;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.metrics.MetricsCollector;
import io.cdap.cdap.common.utils.TimeProvider;
import io.cdap.cdap.messaging.StoreRequest;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.messaging.data.RawMessage;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Unit-test for {@link ConcurrentMessageWriter}.
 */
public class ConcurrentMessageWriterTest {

  private static final Logger LOG = LoggerFactory.getLogger(ConcurrentMessageWriterTest.class);

  @Test
  public void testBasic() throws IOException {
    TopicId topicId1 = new NamespaceId("ns1").topic("t1");
    TopicId topicId2 = new NamespaceId("ns2").topic("t2");

    TopicMetadata metadata1 = new TopicMetadata(topicId1.toSpiTopicId(), new HashMap<String, String>(), 1);
    TopicMetadata metadata2 = new TopicMetadata(topicId2.toSpiTopicId(), new HashMap<String, String>(), 1);

    TestStoreRequestWriter testWriter = new TestStoreRequestWriter(new TimeProvider.IncrementalTimeProvider());
    ConcurrentMessageWriter writer = new ConcurrentMessageWriter(testWriter);
    writer.persist(new TestStoreRequest(topicId1, Arrays.asList("1", "2", "3")), metadata1);

    // There should be 3 messages being written
    List<RawMessage> messages = testWriter.getMessages().get(topicId1);
    Assert.assertEquals(3, messages.size());

    // All messages should be written with timestamp 0
    List<String> payloads = new ArrayList<>();
    for (RawMessage message : messages) {
      Assert.assertEquals(0L, new MessageId(message.getId()).getPublishTimestamp());
      payloads.add(Bytes.toString(message.getPayload()));
    }
    Assert.assertEquals(Arrays.asList("1", "2", "3"), payloads);

    // Write to another topic
    writer.persist(new TestStoreRequest(topicId2, Arrays.asList("a", "b", "c")), metadata2);

    // There should be 3 messages being written to topic2
    messages = testWriter.getMessages().get(topicId2);
    Assert.assertEquals(3, messages.size());

    // All messages should be written with timestamp 1
    payloads.clear();
    for (RawMessage message : messages) {
      Assert.assertEquals(1L, new MessageId(message.getId()).getPublishTimestamp());
      payloads.add(Bytes.toString(message.getPayload()));
    }
    Assert.assertEquals(Arrays.asList("a", "b", "c"), payloads);
  }

  @Test
  public void testMaxSequence() throws IOException {
    // This test the case when a single StoreRequest has more than SEQUENCE_ID_LIMIT (65536) payload.
    // Expected entries beyond the max seqId will be rolled to the next timestamp with seqId reset to start from 0
    // Generate SEQUENCE_ID_LIMIT + 1 payloads
    int msgCount = StoreRequestWriter.SEQUENCE_ID_LIMIT + 1;
    List<String> payloads = new ArrayList<>(msgCount);
    for (int i = 0; i < msgCount; i++) {
      payloads.add(Integer.toString(i));
    }

    TopicId topicId = new NamespaceId("ns1").topic("t1");
    TopicMetadata metadata = new TopicMetadata(topicId.toSpiTopicId(), new HashMap<String, String>(), 1);

    // Write the payloads
    TestStoreRequestWriter testWriter = new TestStoreRequestWriter(new TimeProvider.IncrementalTimeProvider());
    ConcurrentMessageWriter writer = new ConcurrentMessageWriter(testWriter);
    writer.persist(new TestStoreRequest(topicId, payloads), metadata);

    List<RawMessage> messages = testWriter.getMessages().get(topicId);
    Assert.assertEquals(msgCount, messages.size());

    // The first SEQUENCE_ID_LIMIT messages should be with the same timestamp, with seqId from 0 to SEQUENCE_ID_LIMIT
    for (int i = 0; i < StoreRequestWriter.SEQUENCE_ID_LIMIT; i++) {
      MessageId id = new MessageId(messages.get(i).getId());
      Assert.assertEquals(0L, id.getPublishTimestamp());
      Assert.assertEquals((short) i, id.getSequenceId());
    }
    // The (SEQUENCE_ID_LIMIT + 1)th message should have a different timestamp and seqId = 0
    MessageId id = new MessageId(messages.get(msgCount - 1).getId());
    Assert.assertEquals(1L, id.getPublishTimestamp());
    Assert.assertEquals(0, id.getPayloadSequenceId());
  }

  @Test
  public void testMultiMaxSequence() throws IOException, InterruptedException {
    TopicId topicId = new NamespaceId("ns1").topic("t1");
    final TopicMetadata metadata = new TopicMetadata(topicId.toSpiTopicId(), new HashMap<String, String>(), 1);

    // This test the case when multiple StoreRequests combined exceeding the 65536 payload.
    // See testMaxSequence() for more details when it matters
    // Generate 3 StoreRequests, each with 43690 messages
    int msgCount = StoreRequestWriter.SEQUENCE_ID_LIMIT / 3 * 2;
    int requestCount = 3;
    List<StoreRequest> requests = new ArrayList<>();
    for (int i = 0; i < requestCount; i++) {
      List<String> payloads = new ArrayList<>(msgCount);
      for (int j = 0; j < msgCount; j++) {
        payloads.add(Integer.toString(j));
      }
      requests.add(new TestStoreRequest(topicId, payloads));
    }

    TestStoreRequestWriter testWriter = new TestStoreRequestWriter(new TimeProvider.IncrementalTimeProvider());
    // We use a custom metrics collector here to make all the persist calls reached the same latch,
    // since we know that the ConcurrentMessageWriter will emit a metrics "persist.requested" after enqueued but
    // before flushing.
    // This will make all requests batched together
    final CountDownLatch latch = new CountDownLatch(requestCount);
    final ConcurrentMessageWriter writer = new ConcurrentMessageWriter(testWriter, new MetricsCollector() {
      @Override
      public void increment(String metricName, long value) {
        if ("persist.requested".equals(metricName)) {
          latch.countDown();
          Uninterruptibles.awaitUninterruptibly(latch);
        }
      }

      @Override
      public void gauge(String metricName, long value) {
        LOG.info("MetricsContext.gauge: {} = {}", metricName, value);
      }
    });

    ExecutorService executor = Executors.newFixedThreadPool(3);
    for (final StoreRequest request : requests) {
      executor.submit(new Runnable() {
        @Override
        public void run() {
          try {
            writer.persist(request, metadata);
          } catch (IOException e) {
            LOG.error("Failed to persist", e);
          }
        }
      });
    }
    executor.shutdown();
    Assert.assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));

    // Validates all messages are being written
    List<RawMessage> messages = testWriter.getMessages().get(topicId);
    Assert.assertEquals(requestCount * msgCount, messages.size());

    // We expect the payload is in repeated sequence of [0..msgCount-1]
    int expectedPayload = 0;

    // The timestamp should be (i / SEQUENCE_ID_LIMIT)
    // The sequenceId should be (i % SEQUENCE_ID_LIMIT)
    for (int i = 0; i < messages.size(); i++) {
      RawMessage message = messages.get(i);
      MessageId messageId = new MessageId(message.getId());
      Assert.assertEquals(i / StoreRequestWriter.SEQUENCE_ID_LIMIT, messageId.getPublishTimestamp());
      Assert.assertEquals((short) (i % StoreRequestWriter.SEQUENCE_ID_LIMIT), messageId.getSequenceId());

      Assert.assertEquals(expectedPayload, Integer.parseInt(Bytes.toString(message.getPayload())));
      expectedPayload = (expectedPayload + 1) % msgCount;
    }
  }

  @Test
  public void testConcurrentWrites() throws InterruptedException, BrokenBarrierException {
    int payloadsPerRequest = 200;
    int threadCount = 20;
    final int requestPerThread = 20;
    long writeLatencyMillis = 50L;

    final TopicId topicId = NamespaceId.DEFAULT.topic("t");
    final TopicMetadata metadata = new TopicMetadata(topicId.toSpiTopicId(), new HashMap<String, String>(), 1);
    TestStoreRequestWriter testWriter = new TestStoreRequestWriter(new TimeProvider.IncrementalTimeProvider(),
                                                                   writeLatencyMillis);
    final ConcurrentMessageWriter writer = new ConcurrentMessageWriter(testWriter);

    final List<String> payload = new ArrayList<>(payloadsPerRequest);
    for (int i = 0; i < payloadsPerRequest; i++) {
      payload.add(Integer.toString(i));
    }


    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    final CyclicBarrier barrier = new CyclicBarrier(threadCount + 1);
    for (int i = 0; i < threadCount; i++) {
      final int threadId = i;
      executor.submit(new Runnable() {
        @Override
        public void run() {
          Stopwatch stopwatch = new Stopwatch();
          try {
            barrier.await();
            stopwatch.start();
            for (int i = 0; i < requestPerThread; i++) {
              writer.persist(new TestStoreRequest(topicId, payload), metadata);
            }
            LOG.info("Complete time for thread {} is {} ms", threadId, stopwatch.elapsedMillis());
          } catch (Exception e) {
            LOG.error("Exception raised when persisting.", e);
          }
        }
      });
    }

    Stopwatch stopwatch = new Stopwatch();
    barrier.await();
    stopwatch.start();
    executor.shutdown();
    Assert.assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));

    LOG.info("Total time passed: {} ms", stopwatch.elapsedMillis());

    // Validate that the total number of messages written is correct
    List<RawMessage> messages = testWriter.getMessages().get(topicId);
    Assert.assertEquals(payloadsPerRequest * threadCount * requestPerThread, messages.size());

    // The message id must be sorted
    RawMessage lastMessage = null;
    for (RawMessage message : messages) {
      if (lastMessage != null) {
        Assert.assertTrue(Bytes.compareTo(lastMessage.getId(), message.getId()) < 0);
      }
      lastMessage = message;
    }
  }

  /**
   * A {@link StoreRequestWriter} that turns all payloads to {@link RawMessage} and stores it in a List.
   */
  private static final class TestStoreRequestWriter extends StoreRequestWriter<TestEntry> {

    private final ListMultimap<TopicId, RawMessage> messages = ArrayListMultimap.create();
    private long writeDelayMillis;

    TestStoreRequestWriter(TimeProvider timeProvider) {
      super(timeProvider, false);
    }

    /**
     * Constructs a writer that has a write delay to simulate latency in persist to real storage.
     */
    TestStoreRequestWriter(TimeProvider timeProvider, long writeDelayMillis) {
      super(timeProvider, false);
      this.writeDelayMillis = writeDelayMillis;
    }

    @Override
    TestEntry getEntry(TopicMetadata metadata, boolean transactional, long transactionWritePointer,
                       long writeTimestamp, short sequenceId, @Nullable byte[] payload) {
      return new TestEntry(new TopicId(metadata.getTopicId()), transactional, transactionWritePointer, writeTimestamp,
                           sequenceId, payload);
    }

    @Override
    protected void doWrite(Iterator<TestEntry> entries) throws IOException {
      while (entries.hasNext()) {
        TestEntry entry = entries.next();
        byte[] rawId = new byte[MessageId.RAW_ID_SIZE];
        MessageId.putRawId(entry.getWriteTimestamp(), entry.getSequenceId(), 0L, (short) 0, rawId, 0);
        byte[] payload = entry.getPayload();
        messages.put(entry.getTopicId(),
                     new RawMessage(rawId, payload == null ? null : Arrays.copyOf(payload, payload.length)));
      }

      if (writeDelayMillis > 0) {
        Uninterruptibles.sleepUninterruptibly(writeDelayMillis, TimeUnit.MILLISECONDS);
      }
    }

    ListMultimap<TopicId, RawMessage> getMessages() {
      return messages;
    }

    @Override
    public void close() throws IOException {
      // No-op
    }
  }

  /**
   * An entry being by the {@link TestStoreRequestWriter}.
   */
  private static final class TestEntry {
    private final TopicId topicId;
    private final boolean transactional;
    private final long transactionWritePointer;
    private final long writeTimestamp;
    private final short sequenceId;
    private final byte[] payload;

    private TestEntry(TopicId topicId, boolean transactional, long transactionWritePointer,
                      long writeTimestamp, short sequenceId, @Nullable byte[] payload) {
      this.topicId = topicId;
      this.transactional = transactional;
      this.transactionWritePointer = transactionWritePointer;
      this.writeTimestamp = writeTimestamp;
      this.sequenceId = sequenceId;
      this.payload = payload;
    }

    public TopicId getTopicId() {
      return topicId;
    }

    public boolean isTransactional() {
      return transactional;
    }

    public long getTransactionWritePointer() {
      return transactionWritePointer;
    }

    public long getWriteTimestamp() {
      return writeTimestamp;
    }

    public short getSequenceId() {
      return sequenceId;
    }

    @Nullable
    public byte[] getPayload() {
      return payload;
    }
  }

  /**
   * A {@link StoreRequest} that takes a list of Strings as payload.
   */
  private static final class TestStoreRequest extends StoreRequest {

    private final List<String> payloads;

    protected TestStoreRequest(TopicId topicId, List<String> payloads) {
      this(topicId, false, -1L, payloads);
    }

    protected TestStoreRequest(TopicId topicId, boolean transactional,
                               long transactionWritePointer, List<String> payloads) {
      super(topicId, transactional, transactionWritePointer);
      this.payloads = payloads;
    }

    @Override
    public boolean hasPayload() {
      return !payloads.isEmpty();
    }

    @Override
    public Iterator<byte[]> iterator() {
      return payloads.stream().map(Bytes::toBytes).iterator();
    }
  }
}
