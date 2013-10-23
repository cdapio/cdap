package com.continuuity.performance.tx;

import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * TxProvider for in-memory benchmarks.
 */
public class InMemoryTxProvider extends TxProvider {

  private Injector injector;
  private InMemoryTransactionManager txManager;

  @Override
  TransactionSystemClient create() {
    injector = Guice.createInjector (
        new DataFabricModules().getInMemoryModules());
    txManager = injector.getInstance(InMemoryTransactionManager.class);
    txManager.startAndWait();
    return injector.getInstance(TransactionSystemClient.class);
  }

  @Override
  void shutdown(TransactionSystemClient txClient) {
    if (injector != null) {
      injector.getInstance(InMemoryTransactionManager.class).stopAndWait();
    }
    if (txManager != null) {
      txManager.stopAndWait();
    }
  }
}
