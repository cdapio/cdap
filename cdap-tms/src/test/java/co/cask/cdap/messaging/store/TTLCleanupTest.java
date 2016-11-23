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
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Tests to verify the coprocessor TTL cleanup logic.
 */
public abstract class TTLCleanupTest {

  @Test
  public void testPayloadTableCompaction() throws Exception {
    try (MetadataTable metadataTable = getMetadataTable();
         PayloadTable payloadTable = getPayloadTable();
         MessageTable messageTable = getMessageTable()) {
      Assert.assertNotNull(messageTable);
      TopicId topicId = NamespaceId.DEFAULT.topic("t2");
      TopicMetadata topic = new TopicMetadata(topicId, "ttl", "3");
      metadataTable.createTopic(topic);
      List<PayloadTable.Entry> entries = new ArrayList<>();
      entries.add(new TestPayloadEntry(topicId, "payloaddata", 101, (short) 0));
      payloadTable.store(entries.iterator());

      byte[] messageId = new byte[MessageId.RAW_ID_SIZE];
      MessageId.putRawId(0L, (short) 0, 0L, (short) 0, messageId, 0);
      CloseableIterator<PayloadTable.Entry> iterator = payloadTable.fetch(topicId, 101L, new MessageId(messageId), true,
                                                                          Integer.MAX_VALUE);
      Assert.assertTrue(iterator.hasNext());
      PayloadTable.Entry entry = iterator.next();
      Assert.assertFalse(iterator.hasNext());
      Assert.assertEquals("payloaddata", Bytes.toString(entry.getPayload()));
      Assert.assertEquals(101L, entry.getTransactionWritePointer());
      iterator.close();

      forceFlushAndCompact(Table.PAYLOAD);

      //Entry should still be there since ttl has not expired
      iterator = payloadTable.fetch(topicId, 101L, new MessageId(messageId), true, Integer.MAX_VALUE);
      Assert.assertTrue(iterator.hasNext());
      entry = iterator.next();
      Assert.assertFalse(iterator.hasNext());
      Assert.assertEquals("payloaddata", Bytes.toString(entry.getPayload()));
      Assert.assertEquals(101L, entry.getTransactionWritePointer());
      iterator.close();

      TimeUnit.SECONDS.sleep(3);
      forceFlushAndCompact(Table.PAYLOAD);

      iterator = payloadTable.fetch(topicId, 101L, new MessageId(messageId), true, Integer.MAX_VALUE);
      Assert.assertFalse(iterator.hasNext());
      iterator.close();
      metadataTable.deleteTopic(topicId);
    }
  }

  @Test
  public void testMessageTableCompaction() throws Exception {
    try (MetadataTable metadataTable = getMetadataTable();
         MessageTable messageTable = getMessageTable();
         PayloadTable payloadTable = getPayloadTable()) {
      Assert.assertNotNull(payloadTable);
      TopicId topicId = NamespaceId.DEFAULT.topic("t1");
      TopicMetadata topic = new TopicMetadata(topicId, "ttl", "3");
      metadataTable.createTopic(topic);
      List<MessageTable.Entry> entries = new ArrayList<>();
      entries.add(new TestMessageEntry(topicId, "data", 100, (short) 0));
      messageTable.store(entries.iterator());

      // Fetch the entries and make sure we are able to read it
      CloseableIterator<MessageTable.Entry> iterator = messageTable.fetch(topicId, 0, Integer.MAX_VALUE, null);
      Assert.assertTrue(iterator.hasNext());
      MessageTable.Entry entry = iterator.next();
      Assert.assertFalse(iterator.hasNext());
      Assert.assertEquals(100, entry.getTransactionWritePointer());
      Assert.assertEquals("data", Bytes.toString(entry.getPayload()));
      iterator.close();

      forceFlushAndCompact(Table.MESSAGE);

      // Entry should still be there since the ttl has not expired
      iterator = messageTable.fetch(topicId, 0, Integer.MAX_VALUE, null);
      entry = iterator.next();
      Assert.assertFalse(iterator.hasNext());
      Assert.assertEquals(100, entry.getTransactionWritePointer());
      Assert.assertEquals("data", Bytes.toString(entry.getPayload()));
      iterator.close();

      TimeUnit.SECONDS.sleep(3);
      forceFlushAndCompact(Table.MESSAGE);

      iterator = messageTable.fetch(topicId, 0, Integer.MAX_VALUE, null);
      Assert.assertFalse(iterator.hasNext());
      iterator.close();
    }
  }

  protected static enum Table {
    MESSAGE,
    PAYLOAD
  }

  protected abstract void forceFlushAndCompact(Table table) throws Exception;

  protected abstract MetadataTable getMetadataTable() throws Exception;

  protected abstract PayloadTable getPayloadTable() throws Exception;

  protected abstract MessageTable getMessageTable() throws Exception;

  private static class TestPayloadEntry implements PayloadTable.Entry {
    private final TopicId topicId;
    private final String payload;
    private final long txWritePtr;
    private final long writeTimestamp;
    private final short seqId;

    TestPayloadEntry(TopicId topicId, String payload, long txWritePtr, short seqId) {
      this.topicId = topicId;
      this.payload = payload;
      this.txWritePtr = txWritePtr;
      this.writeTimestamp = System.currentTimeMillis();
      this.seqId = seqId;
    }

    @Override
    public TopicId getTopicId() {
      return topicId;
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

  private static class TestMessageEntry implements MessageTable.Entry {
    private final TopicId topicId;
    private final long timestamp;
    private final String payload;
    private final long txWritePtr;
    private final short seqId;

    TestMessageEntry(TopicId topicId, String payload, long txWritePtr, short seqId) {
      this.topicId = topicId;
      this.timestamp = System.currentTimeMillis();
      this.payload = payload;
      this.txWritePtr = txWritePtr;
      this.seqId = seqId;
    }

    @Override
    public TopicId getTopicId() {
      return topicId;
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
