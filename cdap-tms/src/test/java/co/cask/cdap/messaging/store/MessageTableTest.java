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
import org.apache.tephra.Transaction;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
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
      entryList.add(new TestMessageEntry(topicId, true, true, txWritePtr, 0L, (short) 0, Bytes.toBytes(payload)));
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
      Map<Integer, Short> startSequenceIds = new HashMap<>();
      Map<Integer, Short> endSequenceIds = new HashMap<>();
      long publishTimestamp = populateList(entryList, Arrays.asList(100, 101, 102), startSequenceIds, endSequenceIds);
      table.store(entryList.iterator());

      CloseableIterator<MessageTable.Entry> iterator = table.fetch(T1, 0, Integer.MAX_VALUE, null);
      checkPointerCount(iterator, 123, ImmutableSet.of(100L, 101L, 102L), 150);

      // Read with 85 items limit
      iterator = table.fetch(T1, 0, 85, null);
      checkPointerCount(iterator, 123, ImmutableSet.of(100L, 101L, 102L), 85);

      // Read with all messages visible
      Transaction tx = new Transaction(200, 200, new long[] { }, new long[] { }, -1);
      iterator = table.fetch(T1, 0, Integer.MAX_VALUE, tx);
      checkPointerCount(iterator, 123, ImmutableSet.of(100L, 101L, 102L), 150);

      // Read with 101 as invalid transaction
      tx = new Transaction(200, 200, new long[] { 101 }, new long[] { }, -1);
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
      table.delete(T1, publishTimestamp, startSequenceIds.get(101), publishTimestamp, endSequenceIds.get(101));
      iterator = table.fetch(T1, 0, Integer.MAX_VALUE, null);
      checkPointerCount(iterator, 123, ImmutableSet.of(100L, 102L), 100);

      // Delete txPtr entries for 100, and then try fetching transactionally all data
      table.delete(T1, publishTimestamp, startSequenceIds.get(100), publishTimestamp, endSequenceIds.get(100));
      tx = new Transaction(200, 200, new long[] { }, new long[] { }, -1);
      iterator = table.fetch(T1, 0, Integer.MAX_VALUE, tx);
      checkPointerCount(iterator, 123, ImmutableSet.of(102L), 50);

      // Use the above tx and read from t2 and it should give all entries
      iterator = table.fetch(T2, 0, Integer.MAX_VALUE, tx);
      checkPointerCount(iterator, 321, ImmutableSet.of(100L, 101L, 102L), 150);
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

  private long populateList(List<MessageTable.Entry> messageTable, List<Integer> writePointers,
                            Map<Integer, Short> startSequences, Map<Integer, Short> endSequences) {
    int data1 = 123;
    int data2 = 321;

    long timestamp = System.currentTimeMillis();
    short seqId = 0;
    for (Integer writePtr : writePointers) {
      startSequences.put(writePtr, seqId);
      for (int i = 0; i < 50; i++) {
        messageTable.add(new TestMessageEntry(T1, false, true, writePtr, timestamp, seqId++, Bytes.toBytes(data1)));
        messageTable.add(new TestMessageEntry(T2, false, true, writePtr, timestamp, seqId++, Bytes.toBytes(data2)));
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
    private final boolean isPayloadReference;
    private final boolean isTransactional;
    private final long transactionWritePointer;
    private final byte[] payload;
    private final long publishTimestamp;
    private final short sequenceId;

    TestMessageEntry(TopicId topicId, boolean isPayloadReference, boolean isTransactional,
                     long transactionWritePointer, long publishTimestamp, short sequenceId, byte[] payload) {
      this.topicId = topicId;
      this.isPayloadReference = isPayloadReference;
      this.isTransactional = isTransactional;
      this.transactionWritePointer = transactionWritePointer;
      this.publishTimestamp = publishTimestamp;
      this.sequenceId = sequenceId;
      this.payload = payload;
    }

    @Override
    public TopicId getTopicId() {
      return topicId;
    }

    @Override
    public boolean isPayloadReference() {
      return isPayloadReference;
    }

    @Override
    public boolean isTransactional() {
      return isTransactional;
    }

    @Override
    public long getTransactionWritePointer() {
      return transactionWritePointer;
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
