package com.continuuity.data2.transaction.coprocessor;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.snapshot.SnapshotCodecV1;
import com.continuuity.data2.transaction.snapshot.SnapshotCodecV2;
import com.continuuity.data2.util.hbase.ConfigurationTable;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Extends the {@link com.continuuity.data2.transaction.coprocessor.TransactionStateCache} implementation for
 * transaction coprocessors with a version that reads transaction configuration properties from
 * {@link ConfigurationTable}.  This allows the coprocessors to pick up configuration changes without requiring
 * a restart.
 */
public class ReactorTransactionStateCache extends TransactionStateCache {
  // Reactor versions of coprocessors must reference snapshot classes so they get included in generated jar file
  // DO NOT REMOVE
  private static final SnapshotCodecV1 codecV1 = null;
  private static final SnapshotCodecV2 codecV2 = null;

  private String tableNamespace;
  private ConfigurationTable configTable;

  public ReactorTransactionStateCache(String tableNamespace) {
    this.tableNamespace = tableNamespace;
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    this.configTable = new ConfigurationTable(conf);
  }

  protected CConfiguration getSnapshotConfiguration() throws IOException {
    return configTable.read(ConfigurationTable.Type.DEFAULT, tableNamespace);
  }
}
