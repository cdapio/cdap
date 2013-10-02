package com.continuuity.data.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.persist.HDFSTransactionStateStorage;
import com.continuuity.data2.transaction.persist.TransactionStateStorage;
import com.google.inject.AbstractModule;
import com.google.inject.util.Modules;

/**
 * Overrides the bindings for {@code OpexServiceMain} to use.  This is needed so we can provide a new
 * {@link InMemoryTransactionManager} instance each time the
 * {@link com.continuuity.data2.transaction.distributed.TransactionService} starts a new RPC server.
 */
// TODO: remove this and move to a private binding
public class DataFabricOpexModule extends AbstractModule {
  private final CConfiguration conf;

  public DataFabricOpexModule() {
    this(CConfiguration.create());
  }

  public DataFabricOpexModule(CConfiguration conf) {
    this.conf = conf;
  }

  public CConfiguration getConfiguration() {
    return conf;
  }

  @Override
  protected void configure() {
    install(Modules.override(new DataFabricDistributedModule(this.conf)).with(new AbstractModule() {
      @Override
      protected void configure() {
        if (conf.getBoolean(Constants.Transaction.Manager.CFG_DO_PERSIST, true)) {
          bind(TransactionStateStorage.class).to(HDFSTransactionStateStorage.class);
        }
        bind(HDFSTransactionStateStorage.class).toProvider(HDFSTransactionStateStorageProvider.class);
        bind(InMemoryTransactionManager.class).toProvider(InMemoryTransactionManagerProvider.class);
      }
    }));
  }
}
