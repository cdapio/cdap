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

package io.cdap.cdap.messaging.store;

import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Base class for Payload Table tests.
 */
public abstract class PayloadTableTest {

  private static final Logger LOG = LoggerFactory.getLogger(PayloadTable.class);

  private static final TopicId T1 = NamespaceId.DEFAULT.topic("payloadt1");
  private static final TopicId T2 = NamespaceId.DEFAULT.topic("payloadt2");
  private static final int GENERATION = 1;
  private static final Map<String, String> DEFAULT_PROPERTY = ImmutableMap.of(TopicMetadata.TTL_KEY,
                                                                              Integer.toString(10000),
                                                                              TopicMetadata.GENERATION_KEY,
                                                                              Integer.toString(GENERATION));
  private static final TopicMetadata M1 = new TopicMetadata(T1.toSpiTopicId(), DEFAULT_PROPERTY);
  private static final TopicMetadata M2 = new TopicMetadata(T2.toSpiTopicId(), DEFAULT_PROPERTY);

  protected abstract PayloadTable getPayloadTable(TopicMetadata topicMetadata) throws Exception;

  protected abstract MetadataTable getMetadataTable() throws Exception;

  @Test
  public void testSingleMessage() throws Exception {
    TopicId topicId = NamespaceId.DEFAULT.topic("singlePayload");
    TopicMetadata metadata = new TopicMetadata(topicId.toSpiTopicId(), DEFAULT_PROPERTY);
    String payload = "data";
    long txWritePtr = 123L;
    try (MetadataTable metadataTable = getMetadataTable();
         PayloadTable table = getPayloadTable(metadata)) {
      metadataTable.createTopic(metadata);
      List<PayloadTable.Entry> entryList = new ArrayList<>();
      entryList.add(new TestPayloadEntry(topicId, GENERATION, txWritePtr, 1L, (short) 1, Bytes.toBytes(payload)));
      table.store(entryList.iterator());
      byte[] messageId = new byte[MessageId.RAW_ID_SIZE];
      MessageId.putRawId(0L, (short) 0, 0L, (short) 0, messageId, 0);
      try (CloseableIterator<PayloadTable.Entry> iterator = table.fetch(metadata, txWritePtr, new MessageId(messageId),
                                                                        false, Integer.MAX_VALUE)) {
        // Fetch not including the first message, expect empty
        Assert.assertFalse(iterator.hasNext());
      }

      try (CloseableIterator<PayloadTable.Entry> iterator = table.fetch(metadata, txWritePtr, new MessageId(messageId),
                                                                        true, Integer.MAX_VALUE)) {
        // Fetch including the first message
        Assert.assertTrue(iterator.hasNext());
        PayloadTable.Entry entry = iterator.next();
        Assert.assertArrayEquals(Bytes.toBytes(payload), entry.getPayload());
        Assert.assertEquals(txWritePtr, entry.getTransactionWritePointer());
        Assert.assertFalse(iterator.hasNext());
      }
    }
  }

  @Test
  public void testConsumption() throws Exception {
    try (MetadataTable metadataTable = getMetadataTable();
         PayloadTable table1 = getPayloadTable(M1);
         PayloadTable table2 = getPayloadTable(M2)) {
      metadataTable.createTopic(M1);
      metadataTable.createTopic(M2);
      List<PayloadTable.Entry> entryList = new ArrayList<>();
      populateList(entryList);
      table1.store(entryList.iterator());
      table2.store(entryList.iterator());
      byte[] messageId = new byte[MessageId.RAW_ID_SIZE];
      MessageId.putRawId(0L, (short) 0, 0L, (short) 0, messageId, 0);

      // Fetch data with 100 write pointer
      try (CloseableIterator<PayloadTable.Entry> iterator = table1.fetch(M1, 100, new MessageId(messageId), true,
                                                                    Integer.MAX_VALUE)) {
        checkData(iterator, 123, ImmutableSet.of(100L), 50);
      }

      // Fetch only 10 items with 101 write pointer
      try (CloseableIterator<PayloadTable.Entry> iterator = table1.fetch(M1, 101, new MessageId(messageId), true, 1)) {
        checkData(iterator, 123, ImmutableSet.of(101L), 1);
      }

      // Fetch items with 102 write pointer
      try (CloseableIterator<PayloadTable.Entry> iterator = table1.fetch(M1, 102, new MessageId(messageId), true,
                                                                         Integer.MAX_VALUE)) {
        checkData(iterator, 123, ImmutableSet.of(102L), 50);
      }

      // Fetch from t2 with 101 write pointer
      try (CloseableIterator<PayloadTable.Entry> iterator = table2.fetch(M2, 101, new MessageId(messageId), true,
                                                                         Integer.MAX_VALUE)) {
        checkData(iterator, 123, ImmutableSet.of(101L), 50);
      }
    }
  }

  @Test
  public void testConcurrentWrites() throws Exception {
    // Create two threads, each of them writes to a different topic with two events in one store call.
    // The iterators in the two threads would alternate to produce payload. This is for testing CDAP-12013
    ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2));
    final CyclicBarrier barrier = new CyclicBarrier(2);
    final CountDownLatch storeCompletion = new CountDownLatch(2);

    for (int i = 0; i < 2; i++) {
      final TopicId topicId = NamespaceId.DEFAULT.topic("testConcurrentWrites" + i);
      TopicMetadata metadata = new TopicMetadata(topicId.toSpiTopicId(), DEFAULT_PROPERTY);

      try (MetadataTable metadataTable = getMetadataTable()) {
        metadataTable.createTopic(metadata);
      }

      final int threadId = i;
      executor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          try (PayloadTable payloadTable = getPayloadTable(metadata)) {
            payloadTable.store(new AbstractIterator<PayloadTable.Entry>() {
              short messageCount;

              @Override
              protected PayloadTable.Entry computeNext() {
                if (messageCount >= 2) {
                  return endOfData();
                }
                try {
                  barrier.await();
                } catch (Exception e) {
                  throw Throwables.propagate(e);
                }
                return new TestPayloadEntry(topicId, GENERATION, threadId, 0, messageCount,
                                            Bytes.toBytes("message " + threadId + " " + messageCount++));
              }
            });
            storeCompletion.countDown();
          } catch (Exception e) {
            LOG.error("Failed to store to MessageTable", e);
          }

          return null;
        }
      });
    }

    executor.shutdown();
    Assert.assertTrue(storeCompletion.await(5, TimeUnit.SECONDS));

    // Read from each topic. Each topic should have two messages
    for (int i = 0; i < 2; i++) {
      TopicId topicId = NamespaceId.DEFAULT.topic("testConcurrentWrites" + i);
      TopicMetadata metadata = new TopicMetadata(topicId.toSpiTopicId(), DEFAULT_PROPERTY);

      byte[] rawId = new byte[MessageId.RAW_ID_SIZE];
      MessageId.putRawId(0L, (short) 0, 0, (short) 0, rawId, 0);
      MessageId messageId = new MessageId(rawId);

      try (
        PayloadTable payloadTable = getPayloadTable(metadata);
        CloseableIterator<PayloadTable.Entry> iterator = payloadTable.fetch(metadata, i, messageId, true, 10);
      ) {
        List<PayloadTable.Entry> entries = Lists.newArrayList(iterator);
        Assert.assertEquals(2, entries.size());

        int count = 0;
        for (PayloadTable.Entry entry : entries) {
          Assert.assertEquals("message " + i + " " + count++, Bytes.toString(entry.getPayload()));
        }
      }
    }
  }

  private void checkData(CloseableIterator<PayloadTable.Entry> entries, int payload, Set<Long> acceptablePtrs,
                         int expectedCount) {
    int count = 0;
    while (entries.hasNext()) {
      PayloadTable.Entry entry = entries.next();
      Assert.assertTrue(acceptablePtrs.contains(entry.getTransactionWritePointer()));
      Assert.assertArrayEquals(Bytes.toBytes(payload), entry.getPayload());
      count++;
    }
    Assert.assertEquals(expectedCount, count);
  }

  private void populateList(List<PayloadTable.Entry> payloadTable) {
    List<Integer> writePointers = ImmutableList.of(100, 101, 102);
    int data = 123;

    long timestamp = System.currentTimeMillis();
    short seqId = 0;
    for (Integer writePtr : writePointers) {
      for (int i = 0; i < 50; i++) {
        payloadTable.add(new TestPayloadEntry(T1, GENERATION, writePtr, timestamp, seqId++, Bytes.toBytes(data)));
        payloadTable.add(new TestPayloadEntry(T2, GENERATION, writePtr, timestamp, seqId++, Bytes.toBytes(data)));
      }
    }
  }

  private class TestPayloadEntry implements PayloadTable.Entry {
    private final TopicId topicId;
    private final int generation;
    private final byte[] payload;
    private final long transactionWritePointer;
    private final long writeTimestamp;
    private final short seqId;

    TestPayloadEntry(TopicId topicId, int generation, long transactionWritePointer, long writeTimestamp,
                     short seqId, byte[] payload) {
      this.topicId = topicId;
      this.generation = generation;
      this.transactionWritePointer = transactionWritePointer;
      this.writeTimestamp = writeTimestamp;
      this.seqId = seqId;
      this.payload = payload;
    }

    @Override
    public io.cdap.cdap.messaging.data.TopicId getTopicId() {
      return topicId.toSpiTopicId();
    }

    @Override
    public int getGeneration() {
      return generation;
    }

    @Override
    public byte[] getPayload() {
      return payload;
    }

    @Override
    public long getTransactionWritePointer() {
      return transactionWritePointer;
    }

    @Override
    public long getPayloadWriteTimestamp() {
      return writeTimestamp;
    }

    @Override
    public short getPayloadSequenceId() {
      return seqId;
    }
  }
}
