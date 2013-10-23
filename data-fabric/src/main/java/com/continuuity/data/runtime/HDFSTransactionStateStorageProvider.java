package com.continuuity.data.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.persist.HDFSTransactionStateStorage;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;

/**
 * Google Guice Provider for the {@link HDFSTransactionStateStorage} class.  Each call to {@link #get()} will return
 * a new instance of the storage provider.
 */
public class HDFSTransactionStateStorageProvider implements Provider<HDFSTransactionStateStorage> {
  private final CConfiguration conf;
  private final Configuration hConf;

  @Inject
  public HDFSTransactionStateStorageProvider(@Named("TransactionServerConfig") CConfiguration config,
                                             @Named("HBaseOVCTableHandleHConfig") Configuration hConf) {
    this.conf = config;
    this.hConf = hConf;
  }

  @Override
  public HDFSTransactionStateStorage get() {
    return new HDFSTransactionStateStorage(conf, hConf);
  }
}
