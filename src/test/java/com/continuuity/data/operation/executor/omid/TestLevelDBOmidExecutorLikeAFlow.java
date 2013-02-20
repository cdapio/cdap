package com.continuuity.data.operation.executor.omid;

import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestLevelDBOmidExecutorLikeAFlow extends TestOmidExecutorLikeAFlow {

  private static final Injector injector = Guice.createInjector (
      new DataFabricLevelDBModule());

  private static final OmidTransactionalOperationExecutor executor =
      (OmidTransactionalOperationExecutor)injector.getInstance(
          OperationExecutor.class);

  private static final OVCTableHandle handle = executor.getTableHandle();

  @Override
  protected OmidTransactionalOperationExecutor getOmidExecutor() {
    return executor;
  }

  @Override
  protected OVCTableHandle getTableHandle() {
    return handle;
  }

  @Override
  protected int getNumIterations() {
    return 100;
  }

  
}
