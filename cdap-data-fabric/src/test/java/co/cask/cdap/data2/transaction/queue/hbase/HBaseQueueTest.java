/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
package co.cask.cdap.data2.transaction.queue.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.data.hbase.HBaseTestBase;
import co.cask.cdap.data.hbase.HBaseTestFactory;
import co.cask.cdap.data.runtime.DataFabricDistributedModule;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.stream.service.InMemoryStreamMetaStore;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.transaction.queue.QueueTest;
import co.cask.cdap.data2.transaction.queue.hbase.coprocessor.CConfigurationReader;
import co.cask.cdap.data2.transaction.queue.hbase.coprocessor.ConsumerConfigCache;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.ConfigurationTable;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HTableNameConverter;
import co.cask.cdap.data2.util.hbase.HTableNameConverterFactory;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.feeds.service.NoOpNotificationFeedManager;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.TxConstants;
import co.cask.tephra.distributed.TransactionService;
import co.cask.tephra.persist.NoOpTransactionStateStorage;
import co.cask.tephra.persist.TransactionStateStorage;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.NavigableMap;

/**
 * HBase queue tests.
 */
public abstract class HBaseQueueTest extends QueueTest {
  private static final Logger LOG = LoggerFactory.getLogger(QueueTest.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static TransactionService txService;
  private static CConfiguration cConf;
  private static Configuration hConf;

  protected static HBaseTestBase testHBase;
  protected static HBaseTableUtil tableUtil;
  private static ZKClientService zkClientService;

  @BeforeClass
  public static void init() throws Exception {
    // Start hbase
    testHBase = new HBaseTestFactory().get();
    testHBase.startHBase();
    hConf = testHBase.getConfiguration();

    // Customize test configuration
    cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, testHBase.getZkConnectionString());
    cConf.set(TxConstants.Service.CFG_DATA_TX_BIND_PORT,
              Integer.toString(Networks.getRandomPort()));
    cConf.set(Constants.Dataset.TABLE_PREFIX, "test");
    cConf.setBoolean(TxConstants.Manager.CFG_DO_PERSIST, false);
    cConf.set(Constants.CFG_HDFS_USER, System.getProperty("user.name"));
    cConf.setLong(QueueConstants.QUEUE_CONFIG_UPDATE_FREQUENCY, 1L);

    final DataFabricDistributedModule dfModule =
      new DataFabricDistributedModule();
    // turn off persistence in tx manager to get rid of ugly zookeeper warnings
    final Module dataFabricModule = Modules.override(dfModule).with(
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(TransactionStateStorage.class).to(NoOpTransactionStateStorage.class);
        }
      });

    final Injector injector = Guice.createInjector(
      dataFabricModule,
      new ConfigModule(cConf, hConf),
      new ZKClientModule(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new TransactionMetricsModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          try {
            bind(LocationFactory.class).toInstance(new LocalLocationFactory(tmpFolder.newFolder()));
            bind(NotificationFeedManager.class).to(NoOpNotificationFeedManager.class).in(Scopes.SINGLETON);
          } catch (IOException e) {
            throw Throwables.propagate(e);
          }
        }
      },
      Modules.override(new StreamAdminModules().getDistributedModules())
      .with(new AbstractModule() {
        @Override
        protected void configure() {
          // The tests are actually testing stream on queue implementation, hence bind it to the queue implementation
          bind(StreamAdmin.class).to(HBaseStreamAdmin.class);
          bind(StreamMetaStore.class).to(InMemoryStreamMetaStore.class);
        }
      })
    );

    //create HBase namespace
    tableUtil = injector.getInstance(HBaseTableUtil.class);
    tableUtil.createNamespaceIfNotExists(testHBase.getHBaseAdmin(), Constants.SYSTEM_NAMESPACE_ID);
    tableUtil.createNamespaceIfNotExists(testHBase.getHBaseAdmin(), NAMESPACE_ID);
    tableUtil.createNamespaceIfNotExists(testHBase.getHBaseAdmin(), NAMESPACE_ID1);

    ConfigurationTable configTable = new ConfigurationTable(hConf);
    configTable.write(ConfigurationTable.Type.DEFAULT, cConf);

    zkClientService = injector.getInstance(ZKClientService.class);
    zkClientService.startAndWait();

    txService = injector.getInstance(TransactionService.class);
    Thread t = new Thread() {
      @Override
      public void run() {
        txService.start();
      }
    };
    t.start();

    txSystemClient = injector.getInstance(TransactionSystemClient.class);
    queueClientFactory = injector.getInstance(QueueClientFactory.class);
    queueAdmin = injector.getInstance(QueueAdmin.class);
    streamAdmin = injector.getInstance(StreamAdmin.class);
    executorFactory = injector.getInstance(TransactionExecutorFactory.class);
  }

  // TODO: CDAP-1177 Should move to QueueTest after making getApplicationName() etc instance methods in a base class
  @Test
  public void testQueueTableNameFormat() throws Exception {
    QueueName queueName = QueueName.fromFlowlet(Constants.DEFAULT_NAMESPACE, "application1", "flow1", "flowlet1",
                                                "output1");
    TableId tableId = ((HBaseQueueAdmin) queueAdmin).getDataTableId(queueName);
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE_ID, tableId.getNamespace());
    Assert.assertEquals("system.queue.application1.flow1", tableId.getTableName());
    String tableName = tableUtil.createHTableDescriptor(tableId).getNameAsString();
    Assert.assertEquals("application1", HBaseQueueAdmin.getApplicationName(tableName));
    Assert.assertEquals("flow1", HBaseQueueAdmin.getFlowName(tableName));

    queueName = QueueName.fromFlowlet("testNamespace", "application1", "flow1", "flowlet1", "output1");
    tableId = ((HBaseQueueAdmin) queueAdmin).getDataTableId(queueName);
    Assert.assertEquals(Id.Namespace.from("testNamespace"), tableId.getNamespace());
    Assert.assertEquals("system.queue.application1.flow1", tableId.getTableName());
    tableName = tableUtil.createHTableDescriptor(tableId).getNameAsString();
    Assert.assertEquals("application1", HBaseQueueAdmin.getApplicationName(tableName));
    Assert.assertEquals("flow1", HBaseQueueAdmin.getFlowName(tableName));
  }

  @Test
  public void testHTablePreSplitted() throws Exception {
    testHTablePreSplitted((HBaseQueueAdmin) queueAdmin, QueueName.fromFlowlet(Constants.DEFAULT_NAMESPACE, "app",
                                                                              "flow", "flowlet", "out"));
  }

  void testHTablePreSplitted(HBaseQueueAdmin admin, QueueName queueName) throws Exception {
    TableId tableId = admin.getDataTableId(queueName);
    if (!admin.exists(queueName)) {
      admin.create(queueName);
    }
    HTable hTable = tableUtil.createHTable(testHBase.getConfiguration(), tableId);
    Assert.assertEquals("Failed for " + admin.getClass().getName(),
                        QueueConstants.DEFAULT_QUEUE_TABLE_PRESPLITS,
                        hTable.getRegionsInRange(new byte[]{0}, new byte[]{(byte) 0xff}).size());
  }

  @Test
  public void configTest() throws Exception {
    QueueName queueName = QueueName.fromFlowlet(Constants.DEFAULT_NAMESPACE, "app", "flow", "flowlet", "out");
    TableId tableId = ((HBaseQueueClientFactory) queueClientFactory).getConfigTableId(queueName);

    // Set a group info
    queueAdmin.configureGroups(queueName, ImmutableMap.of(1L, 1, 2L, 2, 3L, 3));

    HTable hTable = tableUtil.createHTable(testHBase.getConfiguration(), tableId);
    try {
      byte[] rowKey = queueName.toBytes();
      Result result = hTable.get(new Get(rowKey));

      NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(QueueEntryRow.COLUMN_FAMILY);

      Assert.assertEquals(1 + 2 + 3, familyMap.size());

      // Update the startRow of group 2.
      Put put = new Put(rowKey);
      put.add(QueueEntryRow.COLUMN_FAMILY, HBaseQueueAdmin.getConsumerStateColumn(2L, 0), Bytes.toBytes(4));
      put.add(QueueEntryRow.COLUMN_FAMILY, HBaseQueueAdmin.getConsumerStateColumn(2L, 1), Bytes.toBytes(5));
      hTable.put(put);

      // Add instance to group 2
      queueAdmin.configureInstances(queueName, 2L, 3);

      // All instances should have startRow == smallest of old instances
      result = hTable.get(new Get(rowKey));
      for (int i = 0; i < 3; i++) {
        int startRow = Bytes.toInt(result.getColumnLatest(QueueEntryRow.COLUMN_FAMILY,
                                                          HBaseQueueAdmin.getConsumerStateColumn(2L, i)).getValue());
        Assert.assertEquals(4, startRow);
      }

      // Advance startRow of group 2.
      put = new Put(rowKey);
      put.add(QueueEntryRow.COLUMN_FAMILY, HBaseQueueAdmin.getConsumerStateColumn(2L, 0), Bytes.toBytes(7));
      hTable.put(put);

      // Reduce instances of group 2 through group reconfiguration and also add a new group
      queueAdmin.configureGroups(queueName, ImmutableMap.of(2L, 1, 4L, 1));

      // The remaining instance should have startRow == smallest of all before reduction.
      result = hTable.get(new Get(rowKey));
      int startRow = Bytes.toInt(result.getColumnLatest(QueueEntryRow.COLUMN_FAMILY,
                                                        HBaseQueueAdmin.getConsumerStateColumn(2L, 0)).getValue());
      Assert.assertEquals(4, startRow);

      result = hTable.get(new Get(rowKey));
      familyMap = result.getFamilyMap(QueueEntryRow.COLUMN_FAMILY);

      Assert.assertEquals(2, familyMap.size());

      startRow = Bytes.toInt(result.getColumnLatest(QueueEntryRow.COLUMN_FAMILY,
                                                    HBaseQueueAdmin.getConsumerStateColumn(4L, 0)).getValue());
      Assert.assertEquals(4, startRow);

    } finally {
      hTable.close();
      queueAdmin.dropAllInNamespace(Constants.DEFAULT_NAMESPACE);
    }
  }

  @Override
  protected void verifyConsumerConfigExists(QueueName... queueNames) throws Exception {
    for (QueueName queueName : queueNames) {
      ConsumerConfigCache cache = getConsumerConfigCache(queueName);
      cache.updateCache();
      Assert.assertNotNull("for " + queueName, cache.getConsumerConfig(queueName.toBytes()));
    }
  }

  @Override
  protected void verifyConsumerConfigIsDeleted(QueueName... queueNames) throws Exception {
    for (QueueName queueName : queueNames) {
      // Either the config table doesn't exists, or the consumer config is empty for the given queue
      try {
        ConsumerConfigCache cache = getConsumerConfigCache(queueName);
        cache.updateCache();
        Assert.assertNull("for " + queueName, cache.getConsumerConfig(queueName.toBytes()));
      } catch (TableNotFoundException e) {
        // Expected.
      }
    }
  }

  private ConsumerConfigCache getConsumerConfigCache(QueueName queueName) throws Exception {
    TableId tableId = HBaseQueueAdmin.getConfigTableId(queueName);
    HTableDescriptor htd = tableUtil.createHTable(hConf, tableId).getTableDescriptor();
    String configTableName = htd.getNameAsString();
    byte[] configTableNameBytes = Bytes.toBytes(configTableName);
    HTableNameConverter nameConverter = new HTableNameConverterFactory().get();
    CConfigurationReader cConfReader = new CConfigurationReader(hConf, nameConverter.getSysConfigTablePrefix(htd));
    return ConsumerConfigCache.getInstance(hConf, configTableNameBytes, cConfReader);
  }

  @AfterClass
  public static void finish() throws Exception {
    tableUtil.deleteAllInNamespace(testHBase.getHBaseAdmin(), NAMESPACE_ID, "");
    tableUtil.deleteNamespaceIfExists(testHBase.getHBaseAdmin(), NAMESPACE_ID);

    tableUtil.deleteAllInNamespace(testHBase.getHBaseAdmin(), NAMESPACE_ID1, "");
    tableUtil.deleteNamespaceIfExists(testHBase.getHBaseAdmin(), NAMESPACE_ID1);

    txService.stop();
    testHBase.stopHBase();
    zkClientService.stopAndWait();
  }

  @Override
  protected void forceEviction(QueueName queueName) throws Exception {
    TableId tableId = ((HBaseQueueClientFactory) queueClientFactory).getTableId(queueName);
    byte[] tableName = tableUtil.createHTable(testHBase.getConfiguration(), tableId).getTableName();
    // make sure consumer config cache is updated
    final Class coprocessorClass = tableUtil.getQueueRegionObserverClassForVersion();
    testHBase.forEachRegion(tableName, new Function<HRegion, Object>() {
      public Object apply(HRegion region) {
        try {
          Coprocessor cp = region.getCoprocessorHost().findCoprocessor(coprocessorClass.getName());
          // calling cp.getConfigCache().updateConfig(), NOTE: cannot do normal cast and stuff because cp is loaded
          // by different classloader (corresponds to a cp's jar)
          LOG.info("forcing update cache for HBaseQueueRegionObserver of region: " + region);
          Method getConfigCacheMethod = cp.getClass().getDeclaredMethod("getConfigCache");
          getConfigCacheMethod.setAccessible(true);
          Object configCache = getConfigCacheMethod.invoke(cp);
          Method updateConfigMethod = configCache.getClass().getDeclaredMethod("updateCache");
          updateConfigMethod.setAccessible(true);
          updateConfigMethod.invoke(configCache);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
        return null;
      }
    });

    // Force a table flush to trigger eviction
    testHBase.forceRegionFlush(tableName);
    testHBase.forceRegionCompact(tableName, true);
  }

  @Override
  protected void configureGroups(QueueName queueName, Map<Long, Integer> groupInfo) throws Exception {
    if (queueName.isQueue()) {
      queueAdmin.configureGroups(queueName, groupInfo);
    } else {
      streamAdmin.configureGroups(queueName.toStreamId(), groupInfo);
    }
  }
}
