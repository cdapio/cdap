package com.continuuity.data.operation.executor.benchmark;

import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class MemoryOpexProvider extends OpexProvider {

  OperationExecutor create() {
    Injector injector = Guice.createInjector (
        new DataFabricModules().getInMemoryModules());
    return injector.getInstance(OperationExecutor.class);
  }

}
