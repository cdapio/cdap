/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.NamespaceClientUnitTestModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.kerberos.DefaultOwnerAdmin;
import co.cask.cdap.common.kerberos.OwnerAdmin;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.data.hbase.HBaseTestBase;
import co.cask.cdap.data.hbase.HBaseTestFactory;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.queue.ConsumerGroupConfig;
import co.cask.cdap.data2.queue.DequeueResult;
import co.cask.cdap.data2.queue.DequeueStrategy;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.queue.QueueConsumer;
import co.cask.cdap.data2.queue.QueueEntry;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.queue.QueueConfigurer;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.transaction.queue.QueueMetrics;
import co.cask.cdap.data2.transaction.queue.QueueTest;
import co.cask.cdap.data2.transaction.queue.hbase.coprocessor.CConfigurationReader;
import co.cask.cdap.data2.transaction.queue.hbase.coprocessor.ConsumerConfigCache;
import co.cask.cdap.data2.transaction.queue.hbase.coprocessor.TableNameAwareCacheSupplier;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.ConfigurationTable;
import co.cask.cdap.data2.util.hbase.HBaseDDLExecutorFactory;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HTableNameConverter;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.feeds.service.NoOpNotificationFeedManager;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.security.impersonation.Impersonator;
import co.cask.cdap.security.impersonation.UGIProvider;
import co.cask.cdap.security.impersonation.UnsupportedUGIProvider;
import co.cask.cdap.spi.hbase.HBaseDDLExecutor;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.io.InputSupplier;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.TxConstants;
import org.apache.tephra.distributed.TransactionService;
import org.apache.tephra.persist.TransactionVisibilityState;
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
import java.util.ArrayList;
import java.util.List;

/**
 * HBase queue tests.
 */
public abstract class HBaseQueueTest extends QueueTest {
  private static final Logger LOG = LoggerFactory.getLogger(QueueTest.class);
  private static final String TABLE_PREFIX = "test";

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  @ClassRule
  public static final HBaseTestBase TEST_HBASE = new HBaseTestFactory().get();

  private static TransactionService txService;
  private static CConfiguration cConf;
  private static Configuration hConf;
  private static Injector injector;

  protected static HBaseTableUtil tableUtil;
  private static HBaseAdmin hbaseAdmin;
  private static ZKClientService zkClientService;
  private static HBaseDDLExecutor ddlExecutor;

  @BeforeClass
  public static void init() throws Exception {
    hConf = TEST_HBASE.getConfiguration();

    // Customize test configuration
    cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, TEST_HBASE.getZkConnectionString());
    cConf.set(TxConstants.Service.CFG_DATA_TX_BIND_PORT, Integer.toString(Networks.getRandomPort()));
    cConf.set(Constants.Dataset.TABLE_PREFIX, TABLE_PREFIX);
    cConf.set(Constants.CFG_HDFS_USER, System.getProperty("user.name"));
    cConf.setLong(QueueConstants.QUEUE_CONFIG_UPDATE_FREQUENCY, 10000L);
    // Test with fewer splits than default (16).
    // Fewer splits make the forceEvict runs faster, which makes all queue tests run faster
    cConf.setInt(QueueConstants.ConfigKeys.QUEUE_TABLE_PRESPLITS, 4);
    cConf.setLong(TxConstants.Manager.CFG_TX_TIMEOUT, 100000000L);
    cConf.setLong(TxConstants.Manager.CFG_TX_MAX_TIMEOUT, 100000000L);

    injector = Guice.createInjector(
      new DataFabricModules().getDistributedModules(),
      new ConfigModule(cConf, hConf),
      new ZKClientModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new NamespaceClientUnitTestModule().getModule(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new TransactionMetricsModule(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule(),
      new DataSetsModules().getInMemoryModules(),
      new SystemDatasetRuntimeModule().getDistributedModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(NotificationFeedManager.class).to(NoOpNotificationFeedManager.class).in(Scopes.SINGLETON);
          bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
          bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
        }
      }
    );

    //create HBase namespace
    hbaseAdmin = TEST_HBASE.getHBaseAdmin();
    ddlExecutor = new HBaseDDLExecutorFactory(cConf, hbaseAdmin.getConfiguration()).get();
    tableUtil = injector.getInstance(HBaseTableUtil.class);
    ddlExecutor.createNamespaceIfNotExists(tableUtil.getHBaseNamespace(NamespaceId.SYSTEM));
    ddlExecutor.createNamespaceIfNotExists(tableUtil.getHBaseNamespace(NAMESPACE_ID));
    ddlExecutor.createNamespaceIfNotExists(tableUtil.getHBaseNamespace(NAMESPACE_ID1));

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
    txSystemClient = injector.getInstance(TransactionSystemClient.class);
    queueClientFactory = injector.getInstance(QueueClientFactory.class);
    queueAdmin = injector.getInstance(QueueAdmin.class);
    executorFactory = injector.getInstance(TransactionExecutorFactory.class);
  }

  // TODO (CDAP-7358): remove this class once TEPHRA-182 is fixed.
  protected TransactionManager getTransactionManager() {
    return txService.getTransactionManager();
  }


  // TODO: CDAP-1177 Should move to QueueTest after making getApplicationName() etc instance methods in a base class
  @Test
  public void testQueueTableNameFormat() throws Exception {
    QueueName queueName = QueueName.fromFlowlet(NamespaceId.DEFAULT.getEntityName(), "application1", "flow1",
                                                "flowlet1", "output1");
    HBaseQueueAdmin hbaseQueueAdmin = (HBaseQueueAdmin) queueAdmin;
    TableId tableId = hbaseQueueAdmin.getDataTableId(queueName);
    Assert.assertEquals(NamespaceId.DEFAULT.getEntityName(), tableId.getNamespace());
    Assert.assertEquals("system." + hbaseQueueAdmin.getType() + ".application1.flow1", tableId.getTableName());
    String tableName = tableUtil.buildHTableDescriptor(tableId).build().getNameAsString();
    Assert.assertEquals("application1", HBaseQueueAdmin.getApplicationName(tableName));
    Assert.assertEquals("flow1", HBaseQueueAdmin.getFlowName(tableName));

    queueName = QueueName.fromFlowlet("testNamespace", "application1", "flow1", "flowlet1", "output1");
    tableId = hbaseQueueAdmin.getDataTableId(queueName);
    Assert.assertEquals(String.format("%s_testNamespace", TABLE_PREFIX), tableId.getNamespace());
    Assert.assertEquals("system." + hbaseQueueAdmin.getType() + ".application1.flow1", tableId.getTableName());
    tableName = tableUtil.buildHTableDescriptor(tableId).build().getNameAsString();
    Assert.assertEquals("application1", HBaseQueueAdmin.getApplicationName(tableName));
    Assert.assertEquals("flow1", HBaseQueueAdmin.getFlowName(tableName));
  }

  @Test
  public void testHTablePreSplitted() throws Exception {
    testHTablePreSplitted((HBaseQueueAdmin) queueAdmin, QueueName.fromFlowlet(NamespaceId.DEFAULT.getEntityName(),
                                                                              "app", "flow", "flowlet", "out"));
  }

  void testHTablePreSplitted(HBaseQueueAdmin admin, QueueName queueName) throws Exception {
    TableId tableId = admin.getDataTableId(queueName);
    if (!admin.exists(queueName)) {
      admin.create(queueName);
    }
    try (HTable hTable = tableUtil.createHTable(TEST_HBASE.getConfiguration(), tableId)) {
      Assert.assertEquals("Failed for " + admin.getClass().getName(),
                          cConf.getInt(QueueConstants.ConfigKeys.QUEUE_TABLE_PRESPLITS),
                          hTable.getRegionsInRange(new byte[]{0}, new byte[]{(byte) 0xff}).size());
    }
  }

  @Test
  public void configTest() throws Exception {
    final QueueName queueName = QueueName.fromFlowlet(NamespaceId.DEFAULT.getEntityName(),
                                                      "app", "flow", "flowlet", "configure");
    queueAdmin.create(queueName);

    final List<ConsumerGroupConfig> groupConfigs = ImmutableList.of(
      new ConsumerGroupConfig(1L, 1, DequeueStrategy.FIFO, null),
      new ConsumerGroupConfig(2L, 2, DequeueStrategy.FIFO, null),
      new ConsumerGroupConfig(3L, 3, DequeueStrategy.FIFO, null)
    );

    try (HBaseConsumerStateStore stateStore = ((HBaseQueueAdmin) queueAdmin).getConsumerStateStore(queueName)) {
      TransactionExecutor txExecutor = Transactions.createTransactionExecutor(executorFactory, stateStore);
      // Intentionally set a row state for group 2, instance 0. It's for testing upgrade of config.
      txExecutor.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          stateStore.updateState(2L, 0, QueueEntryRow.getQueueEntryRowKey(queueName, 10L, 0));
        }
      });

      // Set the group info
      configureGroups(queueName, groupConfigs);

      txExecutor.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          for (ConsumerGroupConfig groupConfig : groupConfigs) {
            long groupId = groupConfig.getGroupId();
            List<QueueBarrier> queueBarriers = stateStore.getAllBarriers(groupId);
            Assert.assertEquals(1, queueBarriers.size());

            for (int instanceId = 0; instanceId < groupConfig.getGroupSize(); instanceId++) {
              HBaseConsumerState state = stateStore.getState(groupId, instanceId);

              if (groupId == 2L && instanceId == 0) {
                // For group 2L instance 0, the start row shouldn't be changed.
                // End row should be the same as the first barrier
                Assert.assertEquals(0, Bytes.compareTo(state.getStartRow(),
                                                       QueueEntryRow.getQueueEntryRowKey(queueName, 10L, 0)));
                Assert.assertEquals(0, Bytes.compareTo(state.getNextBarrier(),
                                                       queueBarriers.get(0).getStartRow()));
              } else {
                // For other group, they should have the start row the same as the first barrier info
                Assert.assertEquals(0, Bytes.compareTo(state.getStartRow(),
                                                       queueBarriers.get(0).getStartRow()));
              }
            }
          }
        }
      });

      txExecutor.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          // Check consumers are all processed up to the barrier boundary
          for (long groupId = 1L; groupId <= 3L; groupId++) {
            List<QueueBarrier> queueBarriers = stateStore.getAllBarriers(groupId);
            boolean allConsumed = stateStore.isAllConsumed(groupId, queueBarriers.get(0).getStartRow());
            // For group 2, instance 0 is not consumed up to the boundary yet
            Assert.assertTrue((groupId == 2L) != allConsumed);

            if (groupId == 2L) {
              // Mark group 2, instance 0 as completed the barrier.
              stateStore.completed(groupId, 0);
            }
          }
        }
      });

      txExecutor.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          // After group 2, instance 0 completed the current barrier, all consumers in group 2 should be able to
          // proceed
          List<QueueBarrier> queueBarriers = stateStore.getAllBarriers(2L);
          byte[] startRow = stateStore.getState(2L, 0).getStartRow();
          Assert.assertEquals(0, Bytes.compareTo(startRow, queueBarriers.get(0).getStartRow()));
          Assert.assertTrue(stateStore.isAllConsumed(2L, startRow));
        }
      });

      // Add instance to group 2
      txExecutor.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          stateStore.configureInstances(2L, 3);
        }
      });

      txExecutor.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          List<QueueBarrier> queueBarriers = stateStore.getAllBarriers(2L);
          Assert.assertEquals(2, queueBarriers.size());

          // For existing instances, the start row shouldn't changed.
          for (int instanceId = 0; instanceId < 2; instanceId++) {
            HBaseConsumerState state = stateStore.getState(2L, instanceId);
            Assert.assertEquals(0, Bytes.compareTo(state.getStartRow(), queueBarriers.get(0).getStartRow()));
            Assert.assertEquals(0, Bytes.compareTo(state.getNextBarrier(), queueBarriers.get(1).getStartRow()));

            // Complete the existing instance
            stateStore.completed(2L, instanceId);
          }

          // For new instances, the start row should be the same as the new barrier
          HBaseConsumerState state = stateStore.getState(2L, 2);
          Assert.assertEquals(0, Bytes.compareTo(state.getStartRow(), queueBarriers.get(1).getStartRow()));
          Assert.assertNull(state.getNextBarrier());

          // All instances should be consumed up to the beginning of the last barrier info
          Assert.assertTrue(stateStore.isAllConsumed(2L, queueBarriers.get(1).getStartRow()));
        }
      });

      // Reduce instances of group 2 through group reconfiguration, remove group 1 and 3, add group 4.
      configureGroups(queueName, ImmutableList.of(new ConsumerGroupConfig(2L, 1, DequeueStrategy.FIFO, null),
                                                  new ConsumerGroupConfig(4L, 1, DequeueStrategy.FIFO, null))
      );

      txExecutor.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          // States and barrier info for removed groups should be gone
          try {
            // There should be no barrier info for group 1
            List<QueueBarrier> queueBarriers = stateStore.getAllBarriers(1L);
            Assert.assertTrue(queueBarriers.isEmpty());
            stateStore.getState(1L, 0);
            Assert.fail("Not expected to get state for group 1");
          } catch (Exception e) {
            // Expected
          }
          try {
            // There should be no barrier info for group 3
            List<QueueBarrier> queueBarriers = stateStore.getAllBarriers(3L);
            Assert.assertTrue(queueBarriers.isEmpty());
            stateStore.getState(3L, 0);
            Assert.fail("Not expected to get state for group 3");
          } catch (Exception e) {
            // Expected
          }
          // For group 2, there should be two barrier infos,
          // since all consumers passed the first barrier (groupSize = 2). Only the size = 3 and size = 1 left
          List<QueueBarrier> queueBarriers = stateStore.getAllBarriers(2L);
          Assert.assertEquals(2, queueBarriers.size());

          // Make all consumers (3 of them before reconfigure) in group 2 consumes everything
          for (int instanceId = 0; instanceId < 3; instanceId++) {
            stateStore.completed(2L, instanceId);
          }

          // For the remaining consumer, it should start consuming from the latest barrier
          HBaseConsumerState state = stateStore.getState(2L, 0);
          Assert.assertEquals(0, Bytes.compareTo(state.getStartRow(),
                                                 queueBarriers.get(1).getStartRow()));
          Assert.assertNull(state.getNextBarrier());

          // For removed instances, they should throw exception when retrieving their states
          for (int i = 1; i < 3; i++) {
            try {
              stateStore.getState(2L, i);
              Assert.fail("Not expected to get state for group 2, instance " + i);
            } catch (Exception e) {
              // Expected
            }
          }
        }
      });
    } finally {
      queueAdmin.dropAllInNamespace(NamespaceId.DEFAULT);
    }
  }

  // This test upgrade from old queue (salted base) to new queue (sharded base)
  @Test (timeout = 30000L)
  public void testQueueUpgrade() throws Exception {
    final QueueName queueName = QueueName.fromFlowlet(NamespaceId.DEFAULT.getEntityName(), "app",
                                                      "flow", "flowlet", "upgrade");
    HBaseQueueAdmin hbaseQueueAdmin = (HBaseQueueAdmin) queueAdmin;
    HBaseQueueClientFactory hBaseQueueClientFactory = (HBaseQueueClientFactory) queueClientFactory;

    // Create the old queue table explicitly
    HBaseQueueAdmin oldQueueAdmin = new HBaseQueueAdmin(hConf, cConf, injector.getInstance(LocationFactory.class),
                                                        injector.getInstance(HBaseTableUtil.class),
                                                        injector.getInstance(DatasetFramework.class),
                                                        injector.getInstance(TransactionExecutorFactory.class),
                                                        QueueConstants.QueueType.QUEUE,
                                                        injector.getInstance(NamespaceQueryAdmin.class),
                                                        injector.getInstance(Impersonator.class));
    oldQueueAdmin.create(queueName);

    int buckets = cConf.getInt(QueueConstants.ConfigKeys.QUEUE_TABLE_PRESPLITS);
    try (
      final HBaseQueueProducer oldProducer = hBaseQueueClientFactory.createProducer(
        oldQueueAdmin, queueName, QueueConstants.QueueType.QUEUE,
        QueueMetrics.NOOP_QUEUE_METRICS, new SaltedHBaseQueueStrategy(tableUtil, buckets),
        new ArrayList<ConsumerGroupConfig>())
    ) {
      // Enqueue 10 items to old queue table
      Transactions.createTransactionExecutor(executorFactory, oldProducer)
        .execute(new TransactionExecutor.Subroutine() {
          @Override
          public void apply() throws Exception {
            for (int i = 0; i < 10; i++) {
              oldProducer.enqueue(new QueueEntry("key", i, Bytes.toBytes("Message " + i)));
            }
          }
        });
    }

    // Configure the consumer
    final ConsumerConfig consumerConfig = new ConsumerConfig(0L, 0, 1, DequeueStrategy.HASH, "key");
    try (QueueConfigurer configurer = queueAdmin.getQueueConfigurer(queueName)) {
      Transactions.createTransactionExecutor(executorFactory, configurer).execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          configurer.configureGroups(ImmutableList.of(consumerConfig));
        }
      });
    }

    // explicit set the consumer state to be the lowest start row
    try (HBaseConsumerStateStore stateStore = hbaseQueueAdmin.getConsumerStateStore(queueName)) {
      Transactions.createTransactionExecutor(executorFactory, stateStore).execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          stateStore.updateState(consumerConfig.getGroupId(), consumerConfig.getInstanceId(),
                                 QueueEntryRow.getQueueEntryRowKey(queueName, 0L, 0));
        }
      });
    }

    // Enqueue 10 more items to new queue table
    createEnqueueRunnable(queueName, 10, 1, null).run();

    // Verify both old and new table have 10 rows each
    Assert.assertEquals(10, countRows(hbaseQueueAdmin.getDataTableId(queueName,
                                                                     QueueConstants.QueueType.QUEUE)));
    Assert.assertEquals(10, countRows(hbaseQueueAdmin.getDataTableId(queueName,
                                                                     QueueConstants.QueueType.SHARDED_QUEUE)));

    // Create a consumer. It should see all 20 items
    final List<String> messages = Lists.newArrayList();
    try (final QueueConsumer consumer = queueClientFactory.createConsumer(queueName, consumerConfig, 1)) {
      while (messages.size() != 20) {
        Transactions.createTransactionExecutor(executorFactory, (TransactionAware) consumer)
          .execute(new TransactionExecutor.Subroutine() {
            @Override
            public void apply() throws Exception {
              DequeueResult<byte[]> result = consumer.dequeue(20);
              for (byte[] data : result) {
                messages.add(Bytes.toString(data));
              }
            }
          });
      }
    }

    verifyQueueIsEmpty(queueName, ImmutableList.of(consumerConfig));
  }

  @Test (timeout = 30000L)
  public void testReconfigure() throws Exception {
    final QueueName queueName = QueueName.fromFlowlet(NamespaceId.DEFAULT.getEntityName(),
                                                      "app", "flow", "flowlet", "changeinstances");
    ConsumerGroupConfig groupConfig = new ConsumerGroupConfig(0L, 2, DequeueStrategy.HASH, "key");
    configureGroups(queueName, ImmutableList.of(groupConfig));

    // Enqueue 10 items
    createEnqueueRunnable(queueName, 10, 1, null).run();

    // Map from instance id to items dequeued
    final Multimap<Integer, Integer> dequeued = ArrayListMultimap.create();

    // Consume 2 items for each consumer instances
    for (int instanceId = 0; instanceId < groupConfig.getGroupSize(); instanceId++) {
      final ConsumerConfig consumerConfig = new ConsumerConfig(groupConfig, instanceId);
      try (QueueConsumer consumer = queueClientFactory.createConsumer(queueName, consumerConfig, 1)) {
        Transactions.createTransactionExecutor(executorFactory, (TransactionAware) consumer)
          .execute(new TransactionExecutor.Subroutine() {
            @Override
            public void apply() throws Exception {
              DequeueResult<byte[]> result = consumer.dequeue(2);
              Assert.assertEquals(2, result.size());
              for (byte[] data : result) {
                dequeued.put(consumerConfig.getInstanceId(), Bytes.toInt(data));
              }
            }
          });
      }
    }

    // Increase number of instances to 3
    changeInstances(queueName, 0L, 3);

    // Enqueue 10 more items
    createEnqueueRunnable(queueName, 10, 1, null).run();

    groupConfig = new ConsumerGroupConfig(0L, 3, DequeueStrategy.HASH, "key");

    // Dequeue everything
    while (dequeued.size() != 20) {
      for (int instanceId = 0; instanceId < groupConfig.getGroupSize(); instanceId++) {
        final ConsumerConfig consumerConfig = new ConsumerConfig(groupConfig, instanceId);
        try (QueueConsumer consumer = queueClientFactory.createConsumer(queueName, consumerConfig, 1)) {
          Transactions.createTransactionExecutor(executorFactory, (TransactionAware) consumer)
            .execute(new TransactionExecutor.Subroutine() {
              @Override
              public void apply() throws Exception {
                for (byte[] data : consumer.dequeue(20)) {
                  dequeued.put(consumerConfig.getInstanceId(), Bytes.toInt(data));
                }
              }
            });
        }
      }
    }

    // Instance 0 should see all evens before change instances
    Assert.assertEquals(ImmutableList.of(0, 2, 4, 6, 8, 0, 3, 6, 9), dequeued.get(0));
    // Instance 1 should see all odds before change instances
    Assert.assertEquals(ImmutableList.of(1, 3, 5, 7, 9, 1, 4, 7), dequeued.get(1));
    // Instance 2 should only see entries after change instances
    Assert.assertEquals(ImmutableList.of(2, 5, 8), dequeued.get(2));

    // All consumers should have empty dequeue now
    for (int instanceId = 0; instanceId < groupConfig.getGroupSize(); instanceId++) {
      final ConsumerConfig consumerConfig = new ConsumerConfig(groupConfig, instanceId);
      try (QueueConsumer consumer = queueClientFactory.createConsumer(queueName, consumerConfig, 1)) {
        Transactions.createTransactionExecutor(executorFactory, (TransactionAware) consumer)
          .execute(new TransactionExecutor.Subroutine() {
            @Override
            public void apply() throws Exception {
              DequeueResult<byte[]> result = consumer.dequeue(20);
              Assert.assertTrue(result.isEmpty());
            }
          });
      }
    }

    // Enqueue 6 more items for the 3 instances
    createEnqueueRunnable(queueName, 6, 1, null).run();

    // Reduce to 1 consumer
    changeInstances(queueName, 0L, 1);

    // The consumer 0 should be able to consume all 10 new items
    dequeued.clear();
    final ConsumerConfig consumerConfig = new ConsumerConfig(0L, 0, 1, DequeueStrategy.HASH, "key");
    try (final QueueConsumer consumer = queueClientFactory.createConsumer(queueName, consumerConfig, 1)) {
      while (dequeued.size() != 6) {
        Transactions.createTransactionExecutor(executorFactory, (TransactionAware) consumer)
          .execute(new TransactionExecutor.Subroutine() {
            @Override
            public void apply() throws Exception {
              for (byte[] data : consumer.dequeue(1)) {
                dequeued.put(consumerConfig.getInstanceId(), Bytes.toInt(data));
              }
            }
          });
      }
    }

    Assert.assertEquals(ImmutableList.of(0, 1, 2, 3, 4, 5), dequeued.get(0));
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

  /**
   * Count how many rows are there in the given HBase table.
   */
  private int countRows(TableId tableId) throws Exception {
    try (
      HTable hTable = tableUtil.createHTable(hConf, tableId);
      ResultScanner scanner = hTable.getScanner(QueueEntryRow.COLUMN_FAMILY)
    ) {
      return Iterables.size(scanner);
    }
  }


  private ConsumerConfigCache getConsumerConfigCache(QueueName queueName) throws Exception {
    String tableName = HBaseQueueAdmin.getConfigTableName();
    TableId hTableId = tableUtil.createHTableId(new NamespaceId(queueName.getFirstComponent()), tableName);
    try (HTable hTable = tableUtil.createHTable(hConf, hTableId)) {
      HTableDescriptor htd = hTable.getTableDescriptor();
      final TableName configTableName = htd.getTableName();
      String prefix = htd.getValue(Constants.Dataset.TABLE_PREFIX);
      CConfigurationReader cConfReader
        = new CConfigurationReader(hConf, HTableNameConverter.getSysConfigTablePrefix(prefix));
      return TableNameAwareCacheSupplier.getSupplier(configTableName,
                                                     cConfReader, new Supplier<TransactionVisibilityState>() {
          @Override
          public TransactionVisibilityState get() {
            try {
              return getTransactionManager().getSnapshot();
            } catch (IOException e) {
              throw Throwables.propagate(e);
            }
          }
        }, new InputSupplier<HTableInterface>() {
          @Override
          public HTableInterface getInput() throws IOException {
            return new HTable(hConf, configTableName);
          }
        }).get();
    }
  }

  /**
   * Asks the tx manager to take a snapshot.
   */
  private void takeTxSnapshot() throws Exception {
    TransactionManager transactionManager = getTransactionManager();
    Method doSnapshot = transactionManager.getClass().getDeclaredMethod("doSnapshot", boolean.class);
    doSnapshot.setAccessible(true);
    doSnapshot.invoke(transactionManager, false);

    LOG.info("Read pointer: {}", transactionManager.getCurrentState().getReadPointer());
    LOG.info("Snapshot read pointer: {}", transactionManager.getSnapshot().getReadPointer());
  }

  @AfterClass
  public static void finish() throws Exception {
    tableUtil.deleteAllInNamespace(ddlExecutor, tableUtil.getHBaseNamespace(NAMESPACE_ID),
                                   hbaseAdmin.getConfiguration());
    ddlExecutor.deleteNamespaceIfExists(tableUtil.getHBaseNamespace(NAMESPACE_ID));

    tableUtil.deleteAllInNamespace(ddlExecutor, tableUtil.getHBaseNamespace(NAMESPACE_ID1),
                                   hbaseAdmin.getConfiguration());
    ddlExecutor.deleteNamespaceIfExists(tableUtil.getHBaseNamespace(NAMESPACE_ID1));

    hbaseAdmin.close();
    txService.stop();
    zkClientService.stopAndWait();
  }

  @Override
  protected void forceEviction(QueueName queueName, int numGroups) throws Exception {
    TableId tableId = ((HBaseQueueAdmin) queueAdmin).getDataTableId(queueName);
    byte[] tableName = tableUtil.getHTableDescriptor(hbaseAdmin, tableId).getName();

    // make sure consumer config cache is updated with the latest tx snapshot
    takeTxSnapshot();
    final Class coprocessorClass = tableUtil.getQueueRegionObserverClassForVersion();
    TEST_HBASE.forEachRegion(tableName, new Function<HRegion, Object>() {
      public Object apply(HRegion region) {
        try {
          Coprocessor cp = region.getCoprocessorHost().findCoprocessor(coprocessorClass.getName());
          // calling cp.updateCache(), NOTE: cannot do normal cast and stuff because cp is loaded
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
          Method updateCache = cp.getClass().getDeclaredMethod("updateCache");
          updateCache.setAccessible(true);
          updateCache.invoke(cp);

        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
        return null;
      }
    });

    // Force a table flush to trigger eviction
    TEST_HBASE.forceRegionFlush(tableName);
    TEST_HBASE.forceRegionCompact(tableName, true);
  }

  @Override
  protected void configureGroups(QueueName queueName,
                                 final Iterable<? extends ConsumerGroupConfig> groupConfigs) throws Exception {
    Preconditions.checkArgument(queueName.isQueue(), "Only support queue configuration in queue test.");

    try (QueueConfigurer queueConfigurer = queueAdmin.getQueueConfigurer(queueName)) {
      Transactions.createTransactionExecutor(executorFactory, queueConfigurer)
        .execute(new TransactionExecutor.Subroutine() {
          @Override
          public void apply() throws Exception {
            queueConfigurer.configureGroups(groupConfigs);
          }
        });
    }
  }

  private void changeInstances(QueueName queueName, final long groupId, final int instances) throws Exception {
    Preconditions.checkArgument(queueName.isQueue(), "Only support queue configuration in queue test.");
    try (QueueConfigurer queueConfigurer = queueAdmin.getQueueConfigurer(queueName)) {
      Transactions.createTransactionExecutor(executorFactory, queueConfigurer)
        .execute(new TransactionExecutor.Subroutine() {
          @Override
          public void apply() throws Exception {
            queueConfigurer.configureInstances(groupId, instances);
          }
        });
    }
  }

  @Override
  protected void resetConsumerState(final QueueName queueName, final ConsumerConfig consumerConfig) throws Exception {
    try (HBaseConsumerStateStore stateStore = ((HBaseQueueAdmin) queueAdmin).getConsumerStateStore(queueName)) {
      // Reset consumer to the beginning of the first barrir
      Transactions.createTransactionExecutor(executorFactory, stateStore).execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          byte[] startRow = stateStore.getAllBarriers(consumerConfig.getGroupId()).get(0).getStartRow();
          stateStore.updateState(consumerConfig.getGroupId(), consumerConfig.getInstanceId(), startRow);
        }
      });
    }
  }
}
