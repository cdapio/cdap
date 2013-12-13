package com.continuuity.data2.transaction.persist;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

/**
 * Runs transaction persistence tests against the {@link LocalFileTransactionStateStorage} and
 * {@link LocalFileTransactionLog} implementations.
 */
public class LocalTransactionStateStorageTest extends AbstractTransactionStateStorageTest {
  @ClassRule
  public static TemporaryFolder tmpDir = new TemporaryFolder();

  @Override
  protected CConfiguration getConfiguration(String testName) throws IOException {
    File testDir = tmpDir.newFolder(testName);
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.Transaction.Manager.CFG_TX_SNAPSHOT_LOCAL_DIR, testDir.getAbsolutePath());

    return conf;
  }

  @Override
  protected TransactionStateStorage getStorage(CConfiguration conf) {
    return new LocalFileTransactionStateStorage(conf);
  }
}
