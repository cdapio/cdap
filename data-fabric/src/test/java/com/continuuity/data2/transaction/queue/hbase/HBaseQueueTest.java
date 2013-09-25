/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.queue.QueueName;
import com.continuuity.common.service.ServerException;
import com.continuuity.common.utils.Networks;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.operation.executor.remote.OperationExecutorService;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.persist.NoOpTransactionStateStorage;
import com.continuuity.data2.transaction.persist.TransactionStateStorage;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.QueueConstants;
import com.continuuity.data2.transaction.queue.QueueTest;
import com.continuuity.weave.filesystem.LocalLocationFactory;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NavigableMap;

/**
 * HBase queue tests.
 */
public class HBaseQueueTest extends QueueTest {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseQueueTest.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static OperationExecutorService opexService;
  private static CConfiguration cConf;

  @BeforeClass
  public static void init() throws Exception {
    // Start hbase
    HBaseTestBase.startHBase();
    final DataFabricDistributedModule dfModule =
      new DataFabricDistributedModule(HBaseTestBase.getConfiguration());
    // turn off persistence in tx manager to get rid of ugly zookeeper warnings
    final Module dataFabricModule = Modules.override(dfModule).with(
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(TransactionStateStorage.class).to(NoOpTransactionStateStorage.class);
        }
      });
    // Customize test configuration
    cConf = dfModule.getConfiguration();
    cConf.set(Constants.Zookeeper.QUORUM, HBaseTestBase.getZkConnectionString());
    cConf.set(com.continuuity.data.operation.executor.remote.Constants.CFG_DATA_OPEX_SERVER_PORT,
              Integer.toString(Networks.getRandomPort()));
    cConf.set(DataSetAccessor.CFG_TABLE_PREFIX, "test");
    cConf.setBoolean(Constants.TransactionManager.CFG_DO_PERSIST, false);

    final Injector injector = Guice.createInjector(dataFabricModule, new AbstractModule() {

      @Override
      protected void configure() {
        try {
          bind(LocationFactory.class).toInstance(new LocalLocationFactory(tmpFolder.newFolder()));
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
    });

    // transaction manager is a "service" and must be started
    transactionManager = injector.getInstance(InMemoryTransactionManager.class);
    transactionManager.init();

    opexService = injector.getInstance(OperationExecutorService.class);
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          opexService.start(new String[]{}, cConf);
        } catch (ServerException e) {
          LOG.error("Exception.", e);
        }
      }
    };
    t.start();

    txSystemClient = injector.getInstance(TransactionSystemClient.class);
    queueClientFactory = injector.getInstance(QueueClientFactory.class);
    queueAdmin = injector.getInstance(QueueAdmin.class);
    executorFactory = injector.getInstance(TransactionExecutorFactory.class);
  }

  @Test
  public void testHTablePreSplitted() throws Exception {
    String tableName = ((HBaseQueueClientFactory) queueClientFactory).getHBaseTableName();
    if (!queueAdmin.exists(tableName)) {
      queueAdmin.create(tableName);
    }

    HTable hTable = HBaseTestBase.getHTable(Bytes.toBytes(tableName));
    Assert.assertEquals(QueueConstants.DEFAULT_QUEUE_TABLE_PRESPLITS,
                        hTable.getRegionsInRange(new byte[] {0}, new byte[] {(byte) 0xff}).size());
  }

  @Test
  public void configTest() throws Exception {
    String tableName = ((HBaseQueueClientFactory) queueClientFactory).getHBaseTableName();
    QueueName queueName = QueueName.fromFlowlet("flow", "flowlet", "out");

    // Set a group info
    queueAdmin.configureGroups(queueName, ImmutableMap.of(1L, 1, 2L, 2, 3L, 3));

    HTable hTable = HBaseTestBase.getHTable(Bytes.toBytes(tableName));
    try {
      byte[] rowKey = queueName.toBytes();
      Result result = hTable.get(new Get(rowKey));

      NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(QueueConstants.COLUMN_FAMILY);

      Assert.assertEquals(1 + 2 + 3, familyMap.size());

      // Update the startRow of group 2.
      Put put = new Put(rowKey);
      put.add(QueueConstants.COLUMN_FAMILY, HBaseQueueUtils.getConsumerStateColumn(2L, 0), Bytes.toBytes(4));
      put.add(QueueConstants.COLUMN_FAMILY, HBaseQueueUtils.getConsumerStateColumn(2L, 1), Bytes.toBytes(5));
      hTable.put(put);

      // Add instance to group 2
      queueAdmin.configureInstances(queueName, 2L, 3);

      // The newly added instance should have startRow == smallest of old instances
      result = hTable.get(new Get(rowKey));
      int startRow = Bytes.toInt(result.getColumnLatest(QueueConstants.COLUMN_FAMILY,
                                                        HBaseQueueUtils.getConsumerStateColumn(2L, 2)).getValue());
      Assert.assertEquals(4, startRow);

      // Advance startRow of group 2.
      put = new Put(rowKey);
      put.add(QueueConstants.COLUMN_FAMILY, HBaseQueueUtils.getConsumerStateColumn(2L, 0), Bytes.toBytes(7));
      hTable.put(put);

      // Reduce instances of group 2 through group reconfiguration and also add a new group
      queueAdmin.configureGroups(queueName, ImmutableMap.of(2L, 1, 4L, 1));

      // The remaining instance should have startRow == smallest of all before reduction.
      result = hTable.get(new Get(rowKey));
      startRow = Bytes.toInt(result.getColumnLatest(QueueConstants.COLUMN_FAMILY,
                                                        HBaseQueueUtils.getConsumerStateColumn(2L, 0)).getValue());
      Assert.assertEquals(4, startRow);

      result = hTable.get(new Get(rowKey));
      familyMap = result.getFamilyMap(QueueConstants.COLUMN_FAMILY);

      Assert.assertEquals(2, familyMap.size());

      startRow = Bytes.toInt(result.getColumnLatest(QueueConstants.COLUMN_FAMILY,
                                                    HBaseQueueUtils.getConsumerStateColumn(4L, 0)).getValue());
      Assert.assertEquals(4, startRow);

    } finally {
      hTable.close();
      queueAdmin.dropAll();
    }
  }


  @AfterClass
  public static void finish() throws Exception {
    opexService.stop(true);
    HBaseTestBase.stopHBase();
  }

  @Test
  public void testPrefix() {
    Assert.assertTrue(((HBaseQueueClientFactory) queueClientFactory).getHBaseTableName().startsWith("test."));
  }

  @Override
  protected void verifyQueueIsEmpty(QueueName queueName, int numActualConsumers) throws Exception {
    // Force a table flush to trigger eviction
    String tableName = ((HBaseQueueClientFactory) queueClientFactory).getHBaseTableName();
    HBaseTestBase.getHBaseAdmin().flush(tableName);

    super.verifyQueueIsEmpty(queueName, numActualConsumers);
  }
}
