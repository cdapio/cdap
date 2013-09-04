package com.continuuity.performance.opex;

import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * OpexProvider for in-memory benchmarks.
 */
public class MemoryOpexProvider extends OpexProvider {

  @Override
  OperationExecutor create() {
    Injector injector = Guice.createInjector (
        new DataFabricModules().getInMemoryModules());
    injector.getInstance(InMemoryTransactionManager.class).init();
    return injector.getInstance(OperationExecutor.class);
  }

}
