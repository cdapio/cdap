package com.continuuity.data2.transaction.persist;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;


/**
 * Tests persistence of transaction snapshots and write-ahead logs to HDFS storage, using the
 * {@link HDFSTransactionStateStorage} and {@link HDFSTransactionLog} implementations.
 */
public class HDFSTransactionStateStorageTest extends AbstractTransactionStateStorageTest {
  private static final String TEST_DIR = "/tmp/wal_test";

  private static HBaseTestingUtility testUtil = new HBaseTestingUtility();
  private static Configuration hConf;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    testUtil.startMiniDFSCluster(1);
    hConf = testUtil.getConfiguration();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    testUtil.shutdownMiniDFSCluster();
  }

  @Override
  protected CConfiguration getConfiguration(String testName) throws IOException {
    String localTestDir = TEST_DIR + "/" + testName;
    CConfiguration conf = CConfiguration.create();
    // tests should use the current user for HDFS
    conf.unset(Constants.CFG_HDFS_USER);
    conf.set(Constants.Transaction.Manager.CFG_TX_SNAPSHOT_DIR, localTestDir);
    return conf;
  }

  @Override
  protected AbstractTransactionStateStorage getStorage(CConfiguration conf) {
    return new HDFSTransactionStateStorage(conf, hConf);
  }
}
