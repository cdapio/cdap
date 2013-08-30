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
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.remote.OperationExecutorService;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data2.dataset.lib.table.hbase.HBaseTableUtil;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.QueueConstants;
import com.continuuity.data2.transaction.queue.QueueTest;
import com.continuuity.weave.internal.zookeeper.InMemoryZKServer;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.client.HTable;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HBase queue tests.
 */
public class HBaseQueueTest extends QueueTest {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseQueueTest.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static InMemoryZKServer zkServer;
  private static OperationExecutorService opexService;

  @BeforeClass
  public static void init() throws Exception {
    // Start ZooKeeper
    zkServer = InMemoryZKServer.builder().setDataDir(tmpFolder.newFolder()).build();
    zkServer.startAndWait();

    // Start hbase
    HBaseTestBase.startHBase();

    final DataFabricDistributedModule dataFabricModule =
      new DataFabricDistributedModule(HBaseTestBase.getConfiguration());

    // Customize test configuration
    final CConfiguration cConf = dataFabricModule.getConfiguration();
    cConf.set(Constants.CFG_ZOOKEEPER_ENSEMBLE, zkServer.getConnectionStr());
    cConf.set(com.continuuity.data.operation.executor.remote.Constants.CFG_DATA_OPEX_SERVER_PORT,
              Integer.toString(Networks.getRandomPort()));

    cConf.set(HBaseTableUtil.CFG_TABLE_PREFIX, "test");

    final Injector injector = Guice.createInjector(dataFabricModule);

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

    // Get the remote opex
    opex = injector.getInstance(OperationExecutor.class);
    queueClientFactory = injector.getInstance(QueueClientFactory.class);
    queueAdmin = injector.getInstance(QueueAdmin.class);
  }

  @Test
  public void testHTablePreSplitted() throws Exception {
    queueClientFactory.createProducer(QueueName.from(Bytes.toBytes("foo")));
    HTable hTable = ((HBaseQueueClientFactory) queueClientFactory).createHTable();
    Assert.assertEquals(QueueConstants.DEFAULT_QUEUE_TABLE_PRESPLITS,
                        hTable.getRegionsInRange(new byte[] {0, 0}, new byte[] {0xf, 0xf}).size());
  }

  @AfterClass
  public static void finish() throws Exception {
    opexService.stop(true);
    HBaseTestBase.stopHBase();
    zkServer.stopAndWait();
  }

  @Test
  public void testPrefix() {
    Assert.assertTrue(((HBaseQueueClientFactory) queueClientFactory).getHBaseTableName().startsWith("test_"));
  }

}
