package com.continuuity.data.operation.executor.omid;

import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestHyperSQLOmidTransactionalOperationExecutor
extends TestOmidTransactionalOperationExecutor {

  private static final Injector injector =
      Guice.createInjector(new DataFabricModules().getSingleNodeModules());

  private static final OmidTransactionalOperationExecutor executor =
      (OmidTransactionalOperationExecutor)injector.getInstance(
          OperationExecutor.class);

  @Override
  protected OmidTransactionalOperationExecutor getOmidExecutor() {
    return executor;
  }
  
}
