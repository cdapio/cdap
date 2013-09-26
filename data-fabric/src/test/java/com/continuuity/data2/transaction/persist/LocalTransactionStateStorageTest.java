package com.continuuity.data2.transaction.persist;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import org.junit.BeforeClass;

import java.io.File;

/**
 * Runs transaction persistence tests against the {@link LocalTransactionStateStorage} and
 * {@link LocalTransactionLog} implementations.
 */
public class LocalTransactionStateStorageTest extends AbstractTransactionStateStorageTest {
  private static File tmpDir;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    tmpDir = new File(System.getProperties().getProperty("java.io.tmpdir"));
  }

  @Override
  protected CConfiguration getConfiguration(String testName) {
    File testDir = new File(tmpDir, testName);
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.Transaction.Manager.CFG_TX_SNAPSHOT_LOCAL_DIR, testDir.getAbsolutePath());

    return conf;
  }

  @Override
  protected TransactionStateStorage getStorage(CConfiguration conf) {
    return new LocalTransactionStateStorage(conf);
  }
}
