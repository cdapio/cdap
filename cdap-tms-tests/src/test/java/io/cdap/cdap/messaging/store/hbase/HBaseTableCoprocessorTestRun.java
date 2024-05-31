/*
 * Copyright © 2016-2017 Cask Data, Inc.
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
package io.cdap.cdap.messaging.store.hbase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.guice.NamespaceAdminTestModule;
import io.cdap.cdap.data.hbase.HBaseTestBase;
import io.cdap.cdap.data2.transaction.messaging.coprocessor.hbase10.MessageTableRegionObserver;
import io.cdap.cdap.data2.transaction.messaging.coprocessor.hbase10.PayloadTableRegionObserver;
import io.cdap.cdap.data2.util.TableId;
import io.cdap.cdap.data2.util.hbase.ConfigurationReader;
import io.cdap.cdap.data2.util.hbase.ConfigurationWriter;
import io.cdap.cdap.data2.util.hbase.HBaseDDLExecutorFactory;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtil;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtilFactory;
import io.cdap.cdap.messaging.DefaultTopicMetadata;
import io.cdap.cdap.messaging.spi.TopicMetadata;
import io.cdap.cdap.messaging.store.DataCleanupTest;
import io.cdap.cdap.messaging.store.MessageTable;
import io.cdap.cdap.messaging.store.MetadataTable;
import io.cdap.cdap.messaging.store.PayloadTable;
import io.cdap.cdap.messaging.store.TableFactory;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.spi.hbase.HBaseDDLExecutor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TxConstants;
import org.apache.tephra.manager.InvalidTxList;
import org.apache.tephra.metrics.TxMetricsCollector;
import org.apache.tephra.persist.HDFSTransactionStateStorage;
import org.apache.tephra.persist.TransactionSnapshot;
import org.apache.tephra.snapshot.SnapshotCodecProvider;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link MessageTableRegionObserver} and {@link PayloadTableRegionObserver} coprocessors.
 */
public class HBaseTableCoprocessorTestRun extends DataCleanupTest {
  private static final int GENERATION = 1;

  // 8 versions, 1 hour apart, latest is current ts.
  private static final long[] V;

  static {
    long now = System.currentTimeMillis();
    V = new long[9];
    for (int i = 0; i < V.length; i++) {
      V[i] = (now - TimeUnit.HOURS.toMillis(8 - i)) * TxConstants.MAX_TX_PER_MS;
    }
  }

  @ClassRule
  public static final ExternalResource TEST_BASE = HBaseMessageTestSuite.TEST_BASE;

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final HBaseTestBase HBASE_TEST_BASE = HBaseMessageTestSuite.HBASE_TEST_BASE;
  private static final CConfiguration cConf = CConfiguration.create();
  private static InvalidTxList invalidList = new InvalidTxList();

  private static Configuration hConf;
  private static HBaseAdmin hBaseAdmin;
  private static HBaseTableUtil tableUtil;
  private static TableFactory tableFactory;
  private static HBaseDDLExecutor ddlExecutor;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    hConf = HBASE_TEST_BASE.getConfiguration();
    hConf.set(HBaseTableUtil.CFG_HBASE_TABLE_COMPRESSION, HBaseTableUtil.CompressionType.NONE.name());

    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.CFG_HDFS_NAMESPACE, cConf.get(Constants.CFG_LOCAL_DATA_DIR));
    cConf.set(Constants.CFG_HDFS_USER, System.getProperty("user.name"));
    // Reduce the metadata cache refresh frequency for unit tests
    cConf.set(Constants.MessagingSystem.COPROCESSOR_METADATA_CACHE_UPDATE_FREQUENCY_SECONDS,
              Integer.toString(METADATA_CACHE_EXPIRY));

    hBaseAdmin = HBASE_TEST_BASE.getHBaseAdmin();
    hBaseAdmin.getConfiguration().set(HBaseTableUtil.CFG_HBASE_TABLE_COMPRESSION,
                                      HBaseTableUtil.CompressionType.NONE.name());
    tableUtil = new HBaseTableUtilFactory(cConf).get();
    ddlExecutor = new HBaseDDLExecutorFactory(cConf, hConf).get();
    ddlExecutor.createNamespaceIfNotExists(tableUtil.getHBaseNamespace(NamespaceId.SYSTEM));

    LocationFactory locationFactory = getInjector().getInstance(LocationFactory.class);
    tableFactory = new HBaseTableFactory(cConf, hBaseAdmin.getConfiguration(), tableUtil, locationFactory);

    new ConfigurationWriter(hConf, cConf).write(ConfigurationReader.Type.DEFAULT, cConf);

    // write an initial transaction snapshot
    invalidList.addAll(ImmutableList.of(V[3], V[5], V[7]));
    TransactionSnapshot txSnapshot = TransactionSnapshot.copyFrom(
      System.currentTimeMillis(), V[6] - 1, V[7], invalidList,
      // this will set visibility upper bound to V[6]
      Maps.newTreeMap(ImmutableSortedMap.of(V[6], new TransactionManager.InProgressTx(
        V[6] - 1, Long.MAX_VALUE, TransactionManager.InProgressType.SHORT))),
      new HashMap<>(), new TreeMap<>());
    HDFSTransactionStateStorage tmpStorage =
      new HDFSTransactionStateStorage(hConf, new SnapshotCodecProvider(hConf), new TxMetricsCollector());
    tmpStorage.startAndWait();
    tmpStorage.writeSnapshot(txSnapshot);
    tmpStorage.stopAndWait();
  }

  @Test
  public void
  testInvalidTx() throws Exception {
    TopicId topicId = NamespaceId.DEFAULT.topic("invalidTx");
    TopicMetadata topic = new DefaultTopicMetadata(topicId, DefaultTopicMetadata.TTL_KEY, "1000000",
        DefaultTopicMetadata.GENERATION_KEY, Integer.toString(GENERATION));
    try (MetadataTable metadataTable = getMetadataTable();
         MessageTable messageTable = getMessageTable(topic)) {
      metadataTable.createTopic(topic);
      List<MessageTable.Entry> entries = new ArrayList<>();
      long invalidTxWritePtr = invalidList.toRawList().get(0);
      entries.add(new TestMessageEntry(topicId, GENERATION, "data", invalidTxWritePtr, (short) 0));
      messageTable.store(entries.iterator());

      // Fetch the entries and make sure we are able to read it
      try (CloseableIterator<MessageTable.Entry> iterator = messageTable.fetch(topic, 0, Integer.MAX_VALUE, null)) {
        checkEntry(iterator, invalidTxWritePtr);
      }

      // Fetch the entries with tx and make sure we are able to read it
      Transaction tx = new Transaction(V[8], V[8], new long[0], new long[0], -1);
      try (CloseableIterator<MessageTable.Entry> iterator = messageTable.fetch(topic, 0, Integer.MAX_VALUE, tx)) {
        checkEntry(iterator, invalidTxWritePtr);
      }

      // Now run full compaction
      forceFlushAndCompact(Table.MESSAGE);

      // Try to fetch the entry non-transactionally and the entry should still be there
      try (CloseableIterator<MessageTable.Entry> iterator = messageTable.fetch(topic, 0, Integer.MAX_VALUE, null)) {
        checkEntry(iterator, invalidTxWritePtr);
      }

      // Fetch the entries transactionally and we should see no entries returned
      try (CloseableIterator<MessageTable.Entry> iterator = messageTable.fetch(topic, 0, Integer.MAX_VALUE, tx)) {
        Assert.assertFalse(iterator.hasNext());
      }
      metadataTable.deleteTopic(topicId);

      // Sleep so that the metadata cache expires
      TimeUnit.SECONDS.sleep(3 * METADATA_CACHE_EXPIRY);
      forceFlushAndCompact(Table.MESSAGE);

      // Test deletion of messages from a deleted topic
      try (CloseableIterator<MessageTable.Entry> iterator = messageTable.fetch(topic, 0, Integer.MAX_VALUE, null)) {
        Assert.assertFalse(iterator.hasNext());
      }
    }
  }

  private void checkEntry(CloseableIterator<MessageTable.Entry> iterator, long invalidTxWritePtr) {
    MessageTable.Entry entry = iterator.next();
    Assert.assertFalse(iterator.hasNext());
    Assert.assertEquals(invalidTxWritePtr, entry.getTransactionWritePointer());
    Assert.assertEquals("data", Bytes.toString(entry.getPayload()));
    iterator.close();
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    tableUtil.deleteAllInNamespace(ddlExecutor, tableUtil.getHBaseNamespace(NamespaceId.SYSTEM), hConf);
    ddlExecutor.deleteNamespaceIfExists(tableUtil.getHBaseNamespace(NamespaceId.SYSTEM));
    ddlExecutor.close();
    hBaseAdmin.close();
  }

  @Override
  protected void forceFlushAndCompact(Table table) throws Exception {
    TableId tableId;
    if (table.equals(Table.MESSAGE)) {
      tableId = tableUtil.createHTableId(NamespaceId.SYSTEM, cConf.get(Constants.MessagingSystem.MESSAGE_TABLE_NAME));
    } else {
      tableId = tableUtil.createHTableId(NamespaceId.SYSTEM, cConf.get(Constants.MessagingSystem.PAYLOAD_TABLE_NAME));
    }

    byte[] tableName = tableUtil.getHTableDescriptor(hBaseAdmin, tableId).getName();
    HBASE_TEST_BASE.forceRegionFlush(tableName);
    HBASE_TEST_BASE.forceRegionCompact(tableName, true);
  }

  @Override
  protected MetadataTable getMetadataTable() throws Exception {
    return tableFactory.createMetadataTable();
  }

  @Override
  protected PayloadTable getPayloadTable(TopicMetadata topicMetadata) throws Exception {
    return tableFactory.createPayloadTable(topicMetadata);
  }

  @Override
  protected MessageTable getMessageTable(TopicMetadata topicMetadata) throws Exception {
    return tableFactory.createMessageTable(topicMetadata);
  }

  public static Injector getInjector() {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new NamespaceAdminTestModule(),
      new DFSLocationModule());
  }
}
