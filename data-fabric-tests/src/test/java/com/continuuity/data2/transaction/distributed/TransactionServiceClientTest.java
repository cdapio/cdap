/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.distributed;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.Networks;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.TransactionSystemTest;
import com.continuuity.weave.internal.zookeeper.InMemoryZKServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

/**
 * HBase queue tests.
 */
public class TransactionServiceClientTest extends TransactionSystemTest {
  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static CConfiguration cConf;
  private static InMemoryZKServer zkServer;
  private static TransactionService server;

  @Override
  protected TransactionSystemClient getClient() throws Exception {
    return new TransactionServiceClient(cConf);
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

    server = TransactionServiceTest.createTxService(zkServer.getConnectionStr(), Networks.getRandomPort(),
                                                    hConf, tmpFolder.newFolder());
    server.startAndWait();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    try {
      server.doStop();
    } finally {
      zkServer.stop();
    }
  }
}
