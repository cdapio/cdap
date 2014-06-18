package com.continuuity.data2.transaction.coprocessor;

import com.continuuity.common.conf.CConfiguration;
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
