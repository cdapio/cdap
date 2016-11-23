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
package co.cask.cdap.messaging.store.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.NamespaceClientUnitTestModule;
import co.cask.cdap.data.hbase.HBaseTestBase;
import co.cask.cdap.data2.transaction.messaging.coprocessor.hbase98.MessageTableRegionObserver;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.messaging.store.MessageTable;
import co.cask.cdap.messaging.store.MetadataTable;
import co.cask.cdap.messaging.store.PayloadTable;
import co.cask.cdap.messaging.store.TableFactory;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Tests for {@link HBaseMessageTable} coprocessor {@link MessageTableRegionObserver}.
 */
public class HBaseMessageTTLCoprocessorTestRun {

  @ClassRule
  public static final ExternalResource TEST_BASE = HBaseMessageTestSuite.TEST_BASE;

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final HBaseTestBase HBASE_TEST_BASE = HBaseMessageTestSuite.HBASE_TEST_BASE;
  private static final CConfiguration cConf = CConfiguration.create();

  private static Configuration hConf;
  private static HBaseAdmin hBaseAdmin;
  private static HBaseTableUtil tableUtil;
  private static TableFactory tableFactory;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    hConf = HBASE_TEST_BASE.getConfiguration();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.CFG_HDFS_NAMESPACE, cConf.get(Constants.CFG_LOCAL_DATA_DIR));
    cConf.set(Constants.CFG_HDFS_USER, System.getProperty("user.name"));

    hBaseAdmin = HBASE_TEST_BASE.getHBaseAdmin();
    hBaseAdmin.getConfiguration().set(HBaseTableUtil.CFG_HBASE_TABLE_COMPRESSION,
                                      HBaseTableUtil.CompressionType.NONE.name());
    tableUtil = new HBaseTableUtilFactory(cConf).get();
    tableUtil.createNamespaceIfNotExists(hBaseAdmin, tableUtil.getHBaseNamespace(NamespaceId.SYSTEM));

    LocationFactory locationFactory = getInjector().getInstance(LocationFactory.class);
    tableFactory = new HBaseTableFactory(cConf, hBaseAdmin.getConfiguration(), tableUtil, locationFactory);
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    tableUtil.deleteAllInNamespace(hBaseAdmin, tableUtil.getHBaseNamespace(NamespaceId.SYSTEM));
    tableUtil.deleteNamespaceIfExists(hBaseAdmin, tableUtil.getHBaseNamespace(NamespaceId.SYSTEM));
  }

  @Test
  public void testPayloadTableCompaction() throws Exception {
    try (MetadataTable metadataTable = getMetadataTable();
         PayloadTable payloadTable = getPayloadTable()) {
      TableId tableId = tableUtil.createHTableId(NamespaceId.SYSTEM,
                                                 cConf.get(Constants.MessagingSystem.PAYLOAD_TABLE_NAME));
      TopicId topicId = NamespaceId.DEFAULT.topic("t1");
      byte[] tableName = tableUtil.getHTableDescriptor(hBaseAdmin, tableId).getName();
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

      HBASE_TEST_BASE.forceRegionFlush(tableName);
      HBASE_TEST_BASE.forceRegionCompact(tableName, true);

      //Entry should still be there since ttl has not expired
      iterator = payloadTable.fetch(topicId, 101L, new MessageId(messageId), true, Integer.MAX_VALUE);
      Assert.assertTrue(iterator.hasNext());
      entry = iterator.next();
      Assert.assertFalse(iterator.hasNext());
      Assert.assertEquals("payloaddata", Bytes.toString(entry.getPayload()));
      Assert.assertEquals(101L, entry.getTransactionWritePointer());
      iterator.close();

      TimeUnit.SECONDS.sleep(3);
      HBASE_TEST_BASE.forceRegionFlush(tableName);
      HBASE_TEST_BASE.forceRegionCompact(tableName, true);

      iterator = payloadTable.fetch(topicId, 101L, new MessageId(messageId), true, Integer.MAX_VALUE);
      Assert.assertFalse(iterator.hasNext());
      iterator.close();

      // Update the TTL so that read filter will still try to read the old flushed data
      metadataTable.updateTopic(new TopicMetadata(topicId, "ttl", "60"));
      iterator = payloadTable.fetch(topicId, 101L, new MessageId(messageId), true, Integer.MAX_VALUE);
      Assert.assertFalse(iterator.hasNext());
      iterator.close();
      metadataTable.deleteTopic(topicId);
    }
  }

  @Test
  public void testMessageTableCompaction() throws Exception {
    try (MetadataTable metadataTable = getMetadataTable();
         MessageTable messageTable = getMessageTable()) {
      TableId tableId = tableUtil.createHTableId(NamespaceId.SYSTEM,
                                                 cConf.get(Constants.MessagingSystem.MESSAGE_TABLE_NAME));
      TopicId topicId = NamespaceId.DEFAULT.topic("t1");
      byte[] tableName = tableUtil.getHTableDescriptor(hBaseAdmin, tableId).getName();
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

      HBASE_TEST_BASE.forceRegionFlush(tableName);
      HBASE_TEST_BASE.forceRegionCompact(tableName, true);

      // Entry should still be there since the ttl has not expired
      iterator = messageTable.fetch(topicId, 0, Integer.MAX_VALUE, null);
      entry = iterator.next();
      Assert.assertFalse(iterator.hasNext());
      Assert.assertEquals(100, entry.getTransactionWritePointer());
      Assert.assertEquals("data", Bytes.toString(entry.getPayload()));
      iterator.close();

      TimeUnit.SECONDS.sleep(3);
      HBASE_TEST_BASE.forceRegionFlush(tableName);
      HBASE_TEST_BASE.forceRegionCompact(tableName, true);

      iterator = messageTable.fetch(topicId, 0, Integer.MAX_VALUE, null);
      Assert.assertFalse(iterator.hasNext());
      iterator.close();

      // Update the TTL so that read filter will still try to read the old flushed data
      metadataTable.updateTopic(new TopicMetadata(topicId, "ttl", "60"));
      iterator = messageTable.fetch(topicId, 0, Integer.MAX_VALUE, null);
      Assert.assertFalse(iterator.hasNext());
      iterator.close();
      metadataTable.deleteTopic(topicId);
    }
  }

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

  public static Injector getInjector() {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new NamespaceClientUnitTestModule().getModule(),
      new LocationRuntimeModule().getDistributedModules());
  }

  private MetadataTable getMetadataTable() throws Exception {
    return tableFactory.createMetadataTable(NamespaceId.SYSTEM,
                                            cConf.get(Constants.MessagingSystem.METADATA_TABLE_NAME));
  }

  private PayloadTable getPayloadTable() throws Exception {
    return tableFactory.createPayloadTable(NamespaceId.SYSTEM, cConf.get(Constants.MessagingSystem.PAYLOAD_TABLE_NAME));
  }

  private MessageTable getMessageTable() throws Exception {
    return tableFactory.createMessageTable(NamespaceId.SYSTEM, cConf.get(Constants.MessagingSystem.MESSAGE_TABLE_NAME));
  }

}
