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

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Tests to verify the coprocessor data cleanup logic.
 */
public abstract class DataCleanupTest {
  private static final int GENERATION = 1;
  protected static final int METADATA_CACHE_EXPIRY = 1;

  @Test
  public void testPayloadTTLCleanup() throws Exception {
    TopicId topicId = NamespaceId.DEFAULT.topic("t2");
    TopicMetadata topic = new TopicMetadata(topicId.toSpiTopicId(), "ttl", "3",
            TopicMetadata.GENERATION_KEY, Integer.toString(GENERATION));
    try (MetadataTable metadataTable = getMetadataTable();
         PayloadTable payloadTable = getPayloadTable(topic);
         MessageTable messageTable = getMessageTable(topic)) {
      Assert.assertNotNull(messageTable);
      metadataTable.createTopic(topic);
      List<PayloadTable.Entry> entries = new ArrayList<>();
      entries.add(new TestPayloadEntry(topicId, GENERATION, "payloaddata", 101, (short) 0));
      payloadTable.store(entries.iterator());

      byte[] messageId = new byte[MessageId.RAW_ID_SIZE];
      MessageId.putRawId(0L, (short) 0, 0L, (short) 0, messageId, 0);
      CloseableIterator<PayloadTable.Entry> iterator = payloadTable.fetch(topic, 101L, new MessageId(messageId), true,
                                                                          Integer.MAX_VALUE);
      Assert.assertTrue(iterator.hasNext());
      PayloadTable.Entry entry = iterator.next();
      Assert.assertFalse(iterator.hasNext());
      Assert.assertEquals("payloaddata", Bytes.toString(entry.getPayload()));
      Assert.assertEquals(101L, entry.getTransactionWritePointer());
      iterator.close();

      forceFlushAndCompact(Table.PAYLOAD);

      //Entry should still be there since ttl has not expired
      iterator = payloadTable.fetch(topic, 101L, new MessageId(messageId), true, Integer.MAX_VALUE);
      Assert.assertTrue(iterator.hasNext());
      entry = iterator.next();
      Assert.assertFalse(iterator.hasNext());
      Assert.assertEquals("payloaddata", Bytes.toString(entry.getPayload()));
      Assert.assertEquals(101L, entry.getTransactionWritePointer());
      iterator.close();

      TimeUnit.SECONDS.sleep(3 * METADATA_CACHE_EXPIRY);
      forceFlushAndCompact(Table.PAYLOAD);

      iterator = payloadTable.fetch(topic, 101L, new MessageId(messageId), true, Integer.MAX_VALUE);
      Assert.assertFalse(iterator.hasNext());
      iterator.close();
      metadataTable.deleteTopic(topicId.toSpiTopicId());
    }
  }

  @Test
  public void testMessageTTLCleanup() throws Exception {
    TopicId topicId = NamespaceId.DEFAULT.topic("t1");
    TopicMetadata topic = new TopicMetadata(topicId.toSpiTopicId(), "ttl", "3",
            TopicMetadata.GENERATION_KEY, Integer.toString(GENERATION));
    try (MetadataTable metadataTable = getMetadataTable();
         MessageTable messageTable = getMessageTable(topic);
         PayloadTable payloadTable = getPayloadTable(topic)) {
      Assert.assertNotNull(payloadTable);
      metadataTable.createTopic(topic);
      List<MessageTable.Entry> entries = new ArrayList<>();
      entries.add(new TestMessageEntry(topicId, GENERATION, "data", 100, (short) 0));
      messageTable.store(entries.iterator());

      // Fetch the entries and make sure we are able to read it
      CloseableIterator<MessageTable.Entry> iterator = messageTable.fetch(topic, 0, Integer.MAX_VALUE, null);
      Assert.assertTrue(iterator.hasNext());
      MessageTable.Entry entry = iterator.next();
      Assert.assertFalse(iterator.hasNext());
      Assert.assertEquals(100, entry.getTransactionWritePointer());
      Assert.assertEquals("data", Bytes.toString(entry.getPayload()));
      iterator.close();

      forceFlushAndCompact(Table.MESSAGE);

      // Entry should still be there since the ttl has not expired
      iterator = messageTable.fetch(topic, 0, Integer.MAX_VALUE, null);
      entry = iterator.next();
      Assert.assertFalse(iterator.hasNext());
      Assert.assertEquals(100, entry.getTransactionWritePointer());
      Assert.assertEquals("data", Bytes.toString(entry.getPayload()));
      iterator.close();

      TimeUnit.SECONDS.sleep(3 * METADATA_CACHE_EXPIRY);
      forceFlushAndCompact(Table.MESSAGE);

      iterator = messageTable.fetch(topic, 0, Integer.MAX_VALUE, null);
      Assert.assertFalse(iterator.hasNext());
      iterator.close();
    }
  }

  @Test
  public void testOldGenCleanup() throws Exception {
    TopicId topicId = NamespaceId.DEFAULT.topic("oldGenCleanup");
    TopicMetadata topic = new TopicMetadata(topicId.toSpiTopicId(), TopicMetadata.TTL_KEY, "100000",
                                            TopicMetadata.GENERATION_KEY, Integer.toString(GENERATION));
    try (MetadataTable metadataTable = getMetadataTable()) {
      int txWritePtr = 100;
      metadataTable.createTopic(topic);
      List<MessageTable.Entry> entries = new ArrayList<>();
      List<PayloadTable.Entry> pentries = new ArrayList<>();
      byte[] messageId = new byte[MessageId.RAW_ID_SIZE];
      MessageId.putRawId(0L, (short) 0, 0L, (short) 0, messageId, 0);

      entries.add(new TestMessageEntry(topicId, GENERATION, "data", txWritePtr, (short) 0));
      pentries.add(new TestPayloadEntry(topicId, GENERATION, "data", txWritePtr, (short) 0));

      try (MessageTable messageTable = getMessageTable(topic);
           PayloadTable payloadTable = getPayloadTable(topic)) {
        messageTable.store(entries.iterator());
        payloadTable.store(pentries.iterator());
      }

      // Fetch the entries and make sure we are able to read it
      try (MessageTable messageTable = getMessageTable(topic);
           CloseableIterator<MessageTable.Entry> iterator = messageTable.fetch(topic, 0, Integer.MAX_VALUE, null)) {
        checkMessageEntry(iterator, txWritePtr);
      }

      try (PayloadTable payloadTable = getPayloadTable(topic);
           CloseableIterator<PayloadTable.Entry> iterator = payloadTable.fetch(topic, txWritePtr,
                                                                               new MessageId(messageId), true, 100)) {
        checkPayloadEntry(iterator, txWritePtr);
      }

      // Now run full compaction
      forceFlushAndCompact(Table.MESSAGE);
      forceFlushAndCompact(Table.PAYLOAD);

      // Fetch the entries and make sure we are able to read it
      try (MessageTable messageTable = getMessageTable(topic);
           CloseableIterator<MessageTable.Entry> iterator = messageTable.fetch(topic, 0, Integer.MAX_VALUE, null)) {
        checkMessageEntry(iterator, txWritePtr);
      }

      try (PayloadTable payloadTable = getPayloadTable(topic);
           CloseableIterator<PayloadTable.Entry> iterator = payloadTable.fetch(topic, txWritePtr,
                                                                               new MessageId(messageId), true, 100)) {
        checkPayloadEntry(iterator, txWritePtr);
      }

      // delete the topic and recreate it with an incremented generation
      metadataTable.deleteTopic(topicId.toSpiTopicId());

      Map<String, String> newProperties = new HashMap<>(topic.getProperties());
      newProperties.put(TopicMetadata.GENERATION_KEY, Integer.toString(topic.getGeneration() + 1));
      topic = new TopicMetadata(topicId.toSpiTopicId(), newProperties);
      metadataTable.createTopic(topic);

      // Sleep so that the metadata cache in coprocessor expires
      TimeUnit.SECONDS.sleep(3 * METADATA_CACHE_EXPIRY);
      forceFlushAndCompact(Table.MESSAGE);
      forceFlushAndCompact(Table.PAYLOAD);

      topic = metadataTable.getMetadata(topicId.toSpiTopicId());
      try (MessageTable messageTable = getMessageTable(topic);
           CloseableIterator<MessageTable.Entry> iterator = messageTable.fetch(topic, 0, Integer.MAX_VALUE, null)) {
        Assert.assertFalse(iterator.hasNext());
      }

      try (PayloadTable payloadTable = getPayloadTable(topic);
           CloseableIterator<PayloadTable.Entry> iterator = payloadTable.fetch(topic, txWritePtr,
                                                                               new MessageId(messageId), true, 100)) {
        Assert.assertFalse(iterator.hasNext());
      }
    }
  }

  private void checkMessageEntry(CloseableIterator<MessageTable.Entry> iterator, long txWritePtr) {
    MessageTable.Entry entry = iterator.next();
    Assert.assertFalse(iterator.hasNext());
    Assert.assertEquals(txWritePtr, entry.getTransactionWritePointer());
    Assert.assertEquals("data", Bytes.toString(entry.getPayload()));
    iterator.close();
  }

  private void checkPayloadEntry(CloseableIterator<PayloadTable.Entry> iterator, long txWritePtr) {
    PayloadTable.Entry entry = iterator.next();
    Assert.assertFalse(iterator.hasNext());
    Assert.assertEquals(txWritePtr, entry.getTransactionWritePointer());
    Assert.assertEquals("data", Bytes.toString(entry.getPayload()));
    iterator.close();
  }

  protected enum Table {
    MESSAGE,
    PAYLOAD
  }

  protected abstract void forceFlushAndCompact(Table table) throws Exception;

  protected abstract MetadataTable getMetadataTable() throws Exception;

  protected abstract PayloadTable getPayloadTable(TopicMetadata topicMetadata) throws Exception;

  protected abstract MessageTable getMessageTable(TopicMetadata topicMetadata) throws Exception;

  protected static class TestPayloadEntry implements PayloadTable.Entry {
    private final TopicId topicId;
    private final int generation;
    private final String payload;
    private final long txWritePtr;
    private final long writeTimestamp;
    private final short seqId;

    public TestPayloadEntry(TopicId topicId, int generation, String payload, long txWritePtr, short seqId) {
      this.topicId = topicId;
      this.generation = generation;
      this.payload = payload;
      this.txWritePtr = txWritePtr;
      this.writeTimestamp = System.currentTimeMillis();
      this.seqId = seqId;
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
      return Bytes.toBytes(payload);
    }

    @Override
    public long getTransactionWritePointer() {
      return txWritePtr;
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

  protected static class TestMessageEntry implements MessageTable.Entry {
    private final TopicId topicId;
    private final int generation;
    private final long timestamp;
    private final String payload;
    private final long txWritePtr;
    private final short seqId;

    public TestMessageEntry(TopicId topicId, int generation, String payload, long txWritePtr, short seqId) {
      this.topicId = topicId;
      this.generation = generation;
      this.timestamp = System.currentTimeMillis();
      this.payload = payload;
      this.txWritePtr = txWritePtr;
      this.seqId = seqId;
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
    public boolean isPayloadReference() {
      return false;
    }

    @Override
    public boolean isTransactional() {
      return true;
    }

    @Override
    public long getTransactionWritePointer() {
      return txWritePtr;
    }

    @Nullable
    @Override
    public byte[] getPayload() {
      return Bytes.toBytes(payload);
    }

    @Override
    public long getPublishTimestamp() {
      return timestamp;
    }

    @Override
    public short getSequenceId() {
      return seqId;
    }
  }
}
