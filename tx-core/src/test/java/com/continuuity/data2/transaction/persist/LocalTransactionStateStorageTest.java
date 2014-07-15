package com.continuuity.data2.transaction.persist;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.TxConfiguration;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.transaction.snapshot.DefaultSnapshotCodec;
import com.continuuity.data2.transaction.snapshot.SnapshotCodecProvider;
import org.apache.hadoop.conf.Configuration;
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
  protected Configuration getConfiguration(String testName) throws IOException {
    File testDir = tmpDir.newFolder(testName);
    Configuration conf = TxConfiguration.create();
    conf.set(TxConstants.Manager.CFG_TX_SNAPSHOT_LOCAL_DIR, testDir.getAbsolutePath());
    conf.set(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES, DefaultSnapshotCodec.class.getName());

    return conf;
  }

  @Override
  protected AbstractTransactionStateStorage getStorage(Configuration conf) {
    return new LocalFileTransactionStateStorage(conf, new SnapshotCodecProvider(conf));
  }
}
