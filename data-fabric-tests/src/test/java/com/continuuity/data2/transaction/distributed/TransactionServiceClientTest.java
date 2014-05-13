/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.distributed;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.common.utils.Networks;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.TransactionSystemTest;
import com.continuuity.data2.transaction.persist.SnapshotCodecV2;
import com.continuuity.data2.transaction.persist.TransactionSnapshot;
import com.continuuity.data2.transaction.persist.TransactionStateStorage;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.InputStream;

/**
 * HBase queue tests.
 */
public class TransactionServiceClientTest extends TransactionSystemTest {
  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static CConfiguration cConf;
  private static InMemoryZKServer zkServer;
  private static TransactionService server;
  private static TransactionStateStorage txStateStorage;
  private static ZKClientService zkClient;
  private static Injector injector;

  @Override
  protected TransactionSystemClient getClient() throws Exception {
    return injector.getInstance(TransactionSystemClient.class);
  }

  @Override
  protected TransactionStateStorage getSateStorage() throws Exception {
    return txStateStorage;
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    HBaseTestingUtility hBaseTestingUtility = new HBaseTestingUtility();
    hBaseTestingUtility.startMiniDFSCluster(1);
    Configuration hConf = hBaseTestingUtility.getConfiguration();
    hConf.setBoolean("fs.hdfs.impl.disable.cache", true);

    zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    cConf = CConfiguration.create();
    // tests should use the current user for HDFS
    cConf.unset(Constants.CFG_HDFS_USER);
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    // we want persisting for this test
    cConf.setBoolean(Constants.Transaction.Manager.CFG_DO_PERSIST, true);

    server = TransactionServiceTest.createTxService(zkServer.getConnectionStr(), Networks.getRandomPort(),
                                                    hConf, tmpFolder.newFolder());
    server.startAndWait();

    injector = Guice.createInjector(
      new ConfigModule(cConf),
      new ZKClientModule(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new DataFabricModules(cConf, hConf).getDistributedModules());

    zkClient = injector.getInstance(ZKClientService.class);
    zkClient.startAndWait();

    txStateStorage = injector.getInstance(TransactionStateStorage.class);
    txStateStorage.startAndWait();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    try {
      try {
        server.doStop();
      } finally {
        zkClient.stopAndWait();
        txStateStorage.stopAndWait();
      }
    } finally {
      zkServer.stopAndWait();
      txStateStorage.stopAndWait();
    }
  }

  @Test
  public void testGetSnapshot() throws Exception {
    TransactionSystemClient client = getClient();
    TransactionStateStorage stateStorage = getSateStorage();

    Transaction tx1 = client.startShort();
    long currentTime = System.currentTimeMillis();

    InputStream in = client.getSnapshotInputStream();
    SnapshotCodecV2 codec = new SnapshotCodecV2();
    TransactionSnapshot snapshot = codec.decodeState(in);

    Assert.assertTrue(snapshot.getTimestamp() >= currentTime);
    Assert.assertTrue(snapshot.getInProgress().containsKey(tx1.getWritePointer()));

    // Ensures that getSnapshot didn't persist a snapshot
    TransactionSnapshot snapshotAfter = stateStorage.getLatestSnapshot();
    if (snapshotAfter != null) {
      Assert.assertEquals(1L, snapshotAfter.getWritePointer());
      Assert.assertEquals(0L, snapshotAfter.getReadPointer());
      Assert.assertEquals(0, snapshotAfter.getInvalid().size());
      Assert.assertEquals(0, snapshotAfter.getCommittedChangeSets().size());
      Assert.assertEquals(0, snapshotAfter.getCommittingChangeSets().size());
    }
  }
}
