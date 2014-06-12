/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.common.queue.QueueName;
import com.continuuity.common.utils.Networks;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.hbase.HBaseTestFactory;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.transaction.distributed.TransactionService;
import com.continuuity.data2.transaction.persist.NoOpTransactionStateStorage;
import com.continuuity.data2.transaction.persist.TransactionStateStorage;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.QueueConstants;
import com.continuuity.data2.transaction.queue.QueueEntryRow;
import com.continuuity.data2.transaction.queue.QueueTest;
import com.continuuity.data2.transaction.queue.hbase.coprocessor.ConsumerConfigCache;
import com.continuuity.data2.transaction.runtime.TransactionMetricsModule;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.data2.util.hbase.ConfigurationTable;
import com.continuuity.data2.util.hbase.HBaseTableUtil;
import com.continuuity.data2.util.hbase.HBaseTableUtilFactory;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
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
  private static ConsumerConfigCache configCache;

  private static HBaseTestBase testHBase;
  private static HBaseTableUtil tableUtil;
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
    cConf.set(DataSetAccessor.CFG_TABLE_PREFIX, "test");
    cConf.setBoolean(TxConstants.Manager.CFG_DO_PERSIST, false);
    cConf.unset(Constants.CFG_HDFS_USER);
    cConf.setBoolean(TxConstants.DataJanitor.CFG_TX_JANITOR_ENABLE, false);
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

    ConfigurationTable configTable = new ConfigurationTable(hConf);
    configTable.write(ConfigurationTable.Type.DEFAULT, cConf);

    final Injector injector = Guice.createInjector(dataFabricModule,
                                                   new ConfigModule(cConf, hConf),
                                                   new ZKClientModule(),
                                                   new DiscoveryRuntimeModule().getDistributedModules(),
                                                   new TransactionMetricsModule(),
                                                   new AbstractModule() {

      @Override
      protected void configure() {
        try {
          bind(LocationFactory.class).toInstance(new LocalLocationFactory(tmpFolder.newFolder()));
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
    });

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
    configCache = ConsumerConfigCache.getInstance(
      hConf, Bytes.toBytes(((HBaseQueueAdmin) queueAdmin).getConfigTableName()));

    tableUtil = new HBaseTableUtilFactory().get();
  }

  @Test
  public void testQueueTableNameFormat() throws Exception {
    QueueName queueName = QueueName.fromFlowlet("application1", "flow1", "flowlet1", "output1");
    String tableName = ((HBaseQueueAdmin) queueAdmin).getActualTableName(queueName);
    Assert.assertEquals("application1", HBaseQueueAdmin.getApplicationName(tableName));
    Assert.assertEquals("flow1", HBaseQueueAdmin.getFlowName(tableName));
  }

  @Test
  public void testHTablePreSplitted() throws Exception {
    testHTablePreSplitted((HBaseQueueAdmin) queueAdmin, QueueName.fromFlowlet("app", "flow", "flowlet", "out"));
  }

  void testHTablePreSplitted(HBaseQueueAdmin admin, QueueName queueName) throws Exception {
    String tableName = admin.getActualTableName(queueName);
    if (!admin.exists(queueName.toString())) {
      admin.create(queueName.toString());
    }
    HTable hTable = testHBase.getHTable(Bytes.toBytes(tableName));
    Assert.assertEquals("Failed for " + admin.getClass().getName(),
                        QueueConstants.DEFAULT_QUEUE_TABLE_PRESPLITS,
                        hTable.getRegionsInRange(new byte[]{0}, new byte[]{(byte) 0xff}).size());
  }

  @Test
  public void configTest() throws Exception {
    QueueName queueName = QueueName.fromFlowlet("app", "flow", "flowlet", "out");
    String tableName = ((HBaseQueueClientFactory) queueClientFactory).getConfigTableName(queueName);

    // Set a group info
    queueAdmin.configureGroups(queueName, ImmutableMap.of(1L, 1, 2L, 2, 3L, 3));

    HTable hTable = testHBase.getHTable(Bytes.toBytes(tableName));
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
      queueAdmin.dropAll();
    }
  }

  @Override
  protected void verifyConsumerConfigExists(QueueName... queueNames) throws InterruptedException {
    configCache.updateCache();
    for (QueueName queueName : queueNames) {
      Assert.assertNotNull("for " + queueName, configCache.getConsumerConfig(queueName.toBytes()));
    }
  }

  @Override
  protected void verifyConsumerConfigIsDeleted(QueueName... queueNames) throws InterruptedException {
    configCache.updateCache();
    for (QueueName queueName : queueNames) {
      Assert.assertNull("for " + queueName, configCache.getConsumerConfig(queueName.toBytes()));
    }
  }

  @AfterClass
  public static void finish() throws Exception {
    txService.stop();
    testHBase.stopHBase();
    zkClientService.stopAndWait();
  }

  @Test
  public void testPrefix() {
    String queueTablename = ((HBaseQueueAdmin) queueAdmin).getTableNamePrefix();
    Assert.assertTrue(queueTablename.startsWith("test."));
  }

  @Override
  protected void forceEviction(QueueName queueName) throws Exception {
    byte[] tableName = Bytes.toBytes(((HBaseQueueClientFactory) queueClientFactory).getTableName(queueName));
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
      streamAdmin.configureGroups(queueName, groupInfo);
    }
  }
}
