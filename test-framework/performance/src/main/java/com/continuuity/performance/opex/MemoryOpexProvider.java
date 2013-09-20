package com.continuuity.performance.opex;

import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.omid.OmidTransactionalOperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * OpexProvider for in-memory benchmarks.
 */
public class MemoryOpexProvider extends OpexProvider {

  private Injector injector;
  private OperationExecutor opex;

  @Override
  OperationExecutor create() {
    injector = Guice.createInjector (
        new DataFabricModules().getInMemoryModules());
    injector.getInstance(InMemoryTransactionManager.class).init();
    opex = injector.getInstance(OperationExecutor.class);
    return opex;
  }

  @Override
  void shutdown(OperationExecutor opex) {
    if (injector != null) {
      injector.getInstance(InMemoryTransactionManager.class).close();
    }
    if (opex != null && opex instanceof OmidTransactionalOperationExecutor) {
      ((OmidTransactionalOperationExecutor) opex).shutdown();
    }
  }
}
