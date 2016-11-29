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

package co.cask.cdap.messaging.store;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.apache.tephra.Transaction;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Base class for Message Table tests.
 */
public abstract class MessageTableTest {
  private static final TopicId T1 = NamespaceId.DEFAULT.topic("t1");
  private static final TopicId T2 = NamespaceId.DEFAULT.topic("t2");

  protected abstract MessageTable getMessageTable() throws Exception;

  @Test
  public void testSingleMessage() throws Exception {
    TopicId topicId = NamespaceId.DEFAULT.topic("single");
    String payload = "data";
    long txWritePtr = 123L;
    try (MessageTable table = getMessageTable()) {
      List<MessageTable.Entry> entryList = new ArrayList<>();
      entryList.add(new TestMessageEntry(topicId, 0L, 0, txWritePtr, Bytes.toBytes(payload)));
      table.store(entryList.iterator());
      byte[] messageId = new byte[MessageId.RAW_ID_SIZE];
      MessageId.putRawId(0L, (short) 0, 0L, (short) 0, messageId, 0);

      try (CloseableIterator<MessageTable.Entry> iterator = table.fetch(topicId,
                                                                        new MessageId(messageId), false, 50, null)) {
        // Fetch not including the first message, expect empty
        Assert.assertFalse(iterator.hasNext());
      }

      try (CloseableIterator<MessageTable.Entry> iterator = table.fetch(topicId,
                                                                        new MessageId(messageId), true, 50, null)) {
        // Fetch including the first message, should get the message
        Assert.assertTrue(iterator.hasNext());
        MessageTable.Entry entry = iterator.next();
        Assert.assertArrayEquals(Bytes.toBytes(payload), entry.getPayload());
        Assert.assertFalse(iterator.hasNext());
      }

      try (CloseableIterator<MessageTable.Entry> iterator = table.fetch(topicId, 0, 50, null)) {
        // Fetch by time, should get the entry
        MessageTable.Entry entry = iterator.next();
        Assert.assertArrayEquals(Bytes.toBytes(payload), entry.getPayload());
        Assert.assertFalse(iterator.hasNext());
      }

      table.delete(topicId, 0L, (short) 0, 0L, (short) 0);

      try (CloseableIterator<MessageTable.Entry> iterator = table.fetch(topicId,
                                                                        new MessageId(messageId), true, 50, null)) {
        // After message deleted, expected empty
        Assert.assertFalse(iterator.hasNext());
      }
    }
  }

  @Test
  public void testNonTxAndTxConsumption() throws Exception {
    try (MessageTable table = getMessageTable()) {
      List<MessageTable.Entry> entryList = new ArrayList<>();
      Map<Long, Short> startSequenceIds = new HashMap<>();
      Map<Long, Short> endSequenceIds = new HashMap<>();
      long publishTimestamp = populateList(entryList, Arrays.asList(100L, 101L, 102L),
                                           startSequenceIds, endSequenceIds);
      table.store(entryList.iterator());

      CloseableIterator<MessageTable.Entry> iterator = table.fetch(T1, 0, Integer.MAX_VALUE, null);
      checkPointerCount(iterator, 123, ImmutableSet.of(100L, 101L, 102L), 150);

      // Read with 85 items limit
      iterator = table.fetch(T1, 0, 85, null);
      checkPointerCount(iterator, 123, ImmutableSet.of(100L, 101L, 102L), 85);

      // Read with all messages visible
      Transaction tx = new Transaction(200, 200, new long[0], new long[0], -1);
      iterator = table.fetch(T1, 0, Integer.MAX_VALUE, tx);
      checkPointerCount(iterator, 123, ImmutableSet.of(100L, 101L, 102L), 150);

      // Read with 101 as invalid transaction
      tx = new Transaction(200, 200, new long[] { 101 }, new long[0], -1);
      iterator = table.fetch(T1, 0, Integer.MAX_VALUE, tx);
      checkPointerCount(iterator, 123, ImmutableSet.of(100L, 102L), 100);

      // Mark 101 as in progress transaction, then we shouldn't read past committed transaction which is 100.
      tx = new Transaction(100, 100, new long[] {}, new long[] { 101 }, -1);
      iterator = table.fetch(T1, 0, Integer.MAX_VALUE, tx);
      checkPointerCount(iterator, 123, ImmutableSet.of(100L), 50);

      // Same read as above but with limit of 10 elements
      iterator = table.fetch(T1, 0, 10, tx);
      checkPointerCount(iterator, 123, ImmutableSet.of(100L), 10);

      // Reading non-tx from t2 should provide 150 items
      iterator = table.fetch(T2, 0, Integer.MAX_VALUE, null);
      checkPointerCount(iterator, 321, ImmutableSet.of(100L, 101L, 102L), 150);

      // Delete txPtr entries for 101, and then try fetching again for that
      table.delete(T1, publishTimestamp, startSequenceIds.get(101L), publishTimestamp, endSequenceIds.get(101L));
      iterator = table.fetch(T1, 0, Integer.MAX_VALUE, null);
      checkPointerCount(iterator, 123, ImmutableSet.of(100L, 102L), 100);

      // Delete txPtr entries for 100, and then try fetching transactionally all data
      table.delete(T1, publishTimestamp, startSequenceIds.get(100L), publishTimestamp, endSequenceIds.get(100L));
      tx = new Transaction(200, 200, new long[0], new long[0], -1);
      iterator = table.fetch(T1, 0, Integer.MAX_VALUE, tx);
      checkPointerCount(iterator, 123, ImmutableSet.of(102L), 50);

      // Use the above tx and read from t2 and it should give all entries
      iterator = table.fetch(T2, 0, Integer.MAX_VALUE, tx);
      checkPointerCount(iterator, 321, ImmutableSet.of(100L, 101L, 102L), 150);
    }
  }

  @Test
  public void testEmptyPayload() throws Exception {
    TopicId topicId = NamespaceId.DEFAULT.topic("testEmptyPayload");

    // This test the message table supports for empty payload. This is for the case where message table
    // stores only a reference to the payload table
    try (MessageTable table = getMessageTable()) {
      try {
        table.store(Collections.singleton(new TestMessageEntry(topicId, 1L, 0, null, null)).iterator());
        Assert.fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) {
        // Expected as non-transactional message cannot have null payload
      }

      // For transactional message, ok to have null payload
      table.store(Collections.singleton(new TestMessageEntry(topicId, 1L, 0, 2L, null)).iterator());

      // Fetch the entry to validate
      List<MessageTable.Entry> entries = new ArrayList<>();
      try (CloseableIterator<MessageTable.Entry> iterator = table.fetch(topicId, 0L, Integer.MAX_VALUE, null)) {
        Iterators.addAll(entries, iterator);
      }

      Assert.assertEquals(1, entries.size());

      MessageTable.Entry entry = entries.get(0);

      Assert.assertEquals(1L, entry.getPublishTimestamp());
      Assert.assertEquals(0, entry.getSequenceId());
      Assert.assertTrue(entry.isTransactional());
      Assert.assertEquals(2L, entry.getTransactionWritePointer());
      Assert.assertNull(entry.getPayload());
      Assert.assertTrue(entry.isPayloadReference());
    }
  }

  private void checkPointerCount(CloseableIterator<MessageTable.Entry> entries, int payload,
                                 Set<Long> acceptablePtrs, int expectedCount) {
    int count = 0;
    while (entries.hasNext()) {
      MessageTable.Entry entry = entries.next();
      Assert.assertArrayEquals(Bytes.toBytes(payload), entry.getPayload());
      if (entry.isPayloadReference() || entry.isTransactional()) {
        // fetch should have only acceptable write pointers
        Assert.assertTrue(acceptablePtrs.contains(entry.getTransactionWritePointer()));
      }
      count++;
    }
    Assert.assertEquals(expectedCount, count);
  }

  private long populateList(List<MessageTable.Entry> messageTable, List<Long> writePointers,
                            Map<Long, Short> startSequences, Map<Long, Short> endSequences) {
    int data1 = 123;
    int data2 = 321;

    long timestamp = System.currentTimeMillis();
    short seqId = 0;
    for (Long writePtr : writePointers) {
      startSequences.put(writePtr, seqId);
      for (int i = 0; i < 50; i++) {
        messageTable.add(new TestMessageEntry(T1, timestamp, seqId++, writePtr, Bytes.toBytes(data1)));
        messageTable.add(new TestMessageEntry(T2, timestamp, seqId++, writePtr, Bytes.toBytes(data2)));
      }
      // Need to subtract the seqId with 1 since it is already incremented and we want the seqId being used
      // for the last written entry of this tx write ptr
      endSequences.put(writePtr, (short) (seqId - 1));
    }

    return timestamp;
  }

  // Private class for publishing messages
  private static class TestMessageEntry implements MessageTable.Entry {
    private final TopicId topicId;
    private final Long transactionWritePointer;
    private final byte[] payload;
    private final long publishTimestamp;
    private final short sequenceId;

    TestMessageEntry(TopicId topicId, long publishTimestamp, int sequenceId,
                     @Nullable Long transactionWritePointer, @Nullable byte[] payload) {
      this.topicId = topicId;
      this.transactionWritePointer = transactionWritePointer;
      this.publishTimestamp = publishTimestamp;
      this.sequenceId = (short) sequenceId;
      this.payload = payload;
    }

    @Override
    public TopicId getTopicId() {
      return topicId;
    }

    @Override
    public boolean isPayloadReference() {
      return payload == null;
    }

    @Override
    public boolean isTransactional() {
      return transactionWritePointer != null;
    }

    @Override
    public long getTransactionWritePointer() {
      return transactionWritePointer == null ? -1L : transactionWritePointer;
    }

    @Nullable
    @Override
    public byte[] getPayload() {
      return payload;
    }

    @Override
    public long getPublishTimestamp() {
      return publishTimestamp;
    }

    @Override
    public short getSequenceId() {
      return sequenceId;
    }
  }
}
