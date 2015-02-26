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
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
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
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.CoreDatasetsModule;
import co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseMetricsTableModule;
import co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseTableModule;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.queue.QueueConfigurer;
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
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.TxConstants;
import co.cask.tephra.coprocessor.TransactionStateCache;
import co.cask.tephra.distributed.TransactionService;
import co.cask.tephra.persist.TransactionSnapshot;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.common.reflect.TypeToken;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.HRegion;
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
    cConf.set(Constants.CFG_HDFS_USER, System.getProperty("user.name"));
    cConf.setLong(QueueConstants.QUEUE_CONFIG_UPDATE_FREQUENCY, 1L);

    cConf.setLong(TxConstants.Manager.CFG_TX_TIMEOUT, 100000000L);

    Module dataFabricModule = new DataFabricDistributedModule();
    final Injector injector = Guice.createInjector(
      dataFabricModule,
      new ConfigModule(cConf, hConf),
      new ZKClientModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new TransactionMetricsModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(NotificationFeedManager.class).to(NoOpNotificationFeedManager.class).in(Scopes.SINGLETON);

          // Bind the state store DS
          Map<String, DatasetModule> defaultModules = Maps.newLinkedHashMap();
          defaultModules.put("orderedTable-hbase", new HBaseTableModule());
          defaultModules.put("metricsTable-hbase", new HBaseMetricsTableModule());
          defaultModules.put("core", new CoreDatasetsModule());
          defaultModules.put("queueDataset", new HBaseQueueDatasetModule());

          bind(new TypeLiteral<Map<String, ? extends DatasetModule>>() { })
            .annotatedWith(Names.named("defaultDatasetModules")).toInstance(defaultModules);
          install(new FactoryModuleBuilder()
                    .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                    .build(DatasetDefinitionRegistryFactory.class));
          bind(DatasetFramework.class).to(InMemoryDatasetFramework.class).in(Scopes.SINGLETON);
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

    // create base location
//    injector.getInstance(LocationFactory.class).create("/").mkdirs();

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

    // The TransactionManager should be started by the txService.
    // We just want a reference to that so that we can ask for tx snapshot
    transactionManager = injector.getInstance(TransactionManager.class);
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
    final QueueName queueName = QueueName.fromFlowlet(Constants.DEFAULT_NAMESPACE,
                                                      "app", "flow", "flowlet", "configure");
    // Set a group info
    final Map<Long, Integer> groupInfo = ImmutableMap.of(1L, 1, 2L, 2, 3L, 3);
    configureGroups(queueName, groupInfo);

    final HBaseConsumerStateStore stateStore = ((HBaseQueueAdmin) queueAdmin).getConsumerStateStore(queueName);
    try {
      TransactionExecutor txExecutor = Transactions.createTransactionExecutor(executorFactory, stateStore);
      txExecutor.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          byte[] minStartRow = QueueEntryRow.getQueueEntryRowKey(queueName, 0L, 0);
          for (Map.Entry<Long, Integer> entry : groupInfo.entrySet()) {
            long groupId = entry.getKey();
            for (int instanceId = 0; instanceId < entry.getValue(); instanceId++) {
              // All consumers should have empty start rows
              Assert.assertTrue(
                Bytes.compareTo(minStartRow, stateStore.getState(groupId, instanceId).getStartRow()) == 0);
            }
          }
          // Update the startRow of group 2.
          stateStore.updateState(2L, 0, Bytes.toBytes(4));
          stateStore.updateState(2L, 1, Bytes.toBytes(5));
        }
      });

      // Add instance to group 2
      txExecutor.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          stateStore.configureInstances(2L, 3);
        }
      });

      // All instances should have startRow == smallest of old instances
      txExecutor.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          for (int instanceId = 0; instanceId < 3; instanceId++) {
            int startRow = Bytes.toInt(stateStore.getState(2L, instanceId).getStartRow());
            Assert.assertEquals(4, startRow);
          }
          // Advance startRow of group 2 instance 0.
          stateStore.updateState(2L, 0, Bytes.toBytes(7));
        }
      });

      // Reduce instances of group 2 through group reconfiguration, remove group 1 and 3, add group 4.
      configureGroups(queueName, ImmutableMap.of(2L, 1, 4L, 1));

      // The remaining instance should have startRow == smallest of all before reduction.
      txExecutor.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          int startRow = Bytes.toInt(stateStore.getState(2L, 0).getStartRow());
          Assert.assertEquals(4, startRow);

          // All instances of group 1 and 3 should be gone
          for (long groupId : new long[]{1L, 3L}) {
            for (int instanceId = 0; instanceId < groupInfo.get(groupId); instanceId++) {
              try {
                stateStore.getState(groupId, instanceId);
                Assert.fail("Not expect to get state for group " + groupId + ", instance " + instanceId);
              } catch (Exception e) {
                // expected
              }
            }
          }

          startRow = Bytes.toInt(stateStore.getState(4L, 0).getStartRow());
          Assert.assertEquals(4, startRow);
        }
      });

    } finally {
      stateStore.close();
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
    return ConsumerConfigCache.getInstance(hConf, configTableNameBytes,
                                           cConfReader, new Supplier<TransactionSnapshot>() {
      @Override
      public TransactionSnapshot get() {
        try {
          return transactionManager.getSnapshot();
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
    });
  }

  /**
   * Asks the tx manager to take a snapshot.
   */
  private void takeTxSnapshot() throws Exception {
    Method doSnapshot = transactionManager.getClass().getDeclaredMethod("doSnapshot", boolean.class);
    doSnapshot.setAccessible(true);
    doSnapshot.invoke(transactionManager, false);

    LOG.info("Read pointer: {}", transactionManager.getCurrentState().getReadPointer());
    LOG.info("Snapshot read pointer: {}", transactionManager.getSnapshot().getReadPointer());
  }

  @AfterClass
  public static void finish() throws Exception {
    tableUtil.deleteAllInNamespace(testHBase.getHBaseAdmin(), NAMESPACE_ID);
    tableUtil.deleteNamespaceIfExists(testHBase.getHBaseAdmin(), NAMESPACE_ID);

    tableUtil.deleteAllInNamespace(testHBase.getHBaseAdmin(), NAMESPACE_ID1);
    tableUtil.deleteNamespaceIfExists(testHBase.getHBaseAdmin(), NAMESPACE_ID1);

    txService.stop();
    testHBase.stopHBase();
    zkClientService.stopAndWait();
  }

  @Override
  protected void forceEviction(QueueName queueName, int numGroups) throws Exception {
    TableId tableId = ((HBaseQueueAdmin) queueAdmin).getDataTableId(queueName);
    byte[] tableName = tableUtil.getHTableDescriptor(testHBase.getHBaseAdmin(), tableId).getName();

    // make sure consumer config cache is updated with the latest tx snapshot
    takeTxSnapshot();
    final Class coprocessorClass = tableUtil.getQueueRegionObserverClassForVersion();
    testHBase.forEachRegion(tableName, new Function<HRegion, Object>() {
      public Object apply(HRegion region) {
        try {
          Coprocessor cp = region.getCoprocessorHost().findCoprocessor(coprocessorClass.getName());
          // calling cp.getConfigCache().updateConfig(), NOTE: cannot do normal cast and stuff because cp is loaded
          // by different classloader (corresponds to a cp's jar)
          LOG.info("forcing update of transaction state cache for HBaseQueueRegionObserver of region: {}", region);
          Method getTxStateCache = cp.getClass().getDeclaredMethod("getTxStateCache");
          getTxStateCache.setAccessible(true);
          Object txStateCache = getTxStateCache.invoke(cp);
          // the one returned is of type DefaultTransactionStateCache.
          // The refreshState method is a private method of its parent, TransactionStateCache
          Method refreshState = txStateCache.getClass().getSuperclass().getDeclaredMethod("refreshState");
          refreshState.setAccessible(true);
          refreshState.invoke(txStateCache);

          LOG.info("forcing update cache for HBaseQueueRegionObserver of region: {}", region);
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
  protected void configureGroups(QueueName queueName, final Map<Long, Integer> groupInfo) throws Exception {
    if (queueName.isQueue()) {
      final QueueConfigurer queueConfigurer = queueAdmin.getQueueConfigurer(queueName);
      try {
        Transactions.createTransactionExecutor(executorFactory, queueConfigurer)
          .execute(new TransactionExecutor.Subroutine() {
            @Override
            public void apply() throws Exception {
              queueConfigurer.configureGroups(groupInfo);
            }
          });
      } finally {
        Closeables.closeQuietly(queueConfigurer);
      }
    } else {
      streamAdmin.configureGroups(queueName.toStreamId(), groupInfo);
    }
  }

  @Override
  protected void resetConsumerState(QueueName queueName, final ConsumerConfig consumerConfig) throws Exception {
    final HBaseConsumerStateStore stateStore = ((HBaseQueueAdmin) queueAdmin).getConsumerStateStore(queueName);
    try {
      Transactions.createTransactionExecutor(executorFactory, stateStore).execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          stateStore.updateState(consumerConfig.getGroupId(), consumerConfig.getInstanceId(), Bytes.EMPTY_BYTE_ARRAY);
        }
      });
    } finally {
      Closeables.closeQuietly(stateStore);
    }
  }
}
