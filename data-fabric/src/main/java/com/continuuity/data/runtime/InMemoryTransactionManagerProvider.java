package com.continuuity.data.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.persist.TransactionStateStorage;
import com.google.inject.Inject;
import com.google.inject.Provider;

/**
 * Google Guice Provider for {@link InMemoryTransactionManager} instances.  Each call to {@link #get()} will
 * return a new {@link InMemoryTransactionManager} instance.
 */
public class InMemoryTransactionManagerProvider implements Provider<InMemoryTransactionManager> {
  private final CConfiguration conf;
  private final Provider<TransactionStateStorage> storageProvider;

  @Inject
  public InMemoryTransactionManagerProvider(CConfiguration config, Provider<TransactionStateStorage> storageProvider) {
    this.conf = config;
    this.storageProvider = storageProvider;
  }

  @Override
  public InMemoryTransactionManager get() {
    return new InMemoryTransactionManager(conf, storageProvider.get());
  }
}
