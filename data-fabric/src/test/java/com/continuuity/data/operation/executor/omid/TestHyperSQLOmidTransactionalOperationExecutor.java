package com.continuuity.data.operation.executor.omid;

import com.continuuity.data.engine.hypersql.HyperSQLOVCTableHandle;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricLocalModule;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;

import static org.junit.Assert.assertTrue;

public class TestHyperSQLOmidTransactionalOperationExecutor
extends TestOmidTransactionalOperationExecutor {

  private static final Injector injector = Guice.createInjector (
      new DataFabricLocalModule("jdbc:hsqldb:mem:membenchdb", null));
  // Guice.createInjector(new DataFabricModules().getSingleNodeModules());

  private static final OmidTransactionalOperationExecutor executor =
      (OmidTransactionalOperationExecutor)injector.getInstance(
          OperationExecutor.class);

  @Override
  protected OmidTransactionalOperationExecutor getOmidExecutor() {
    return executor;
  }

  @Override
  public void testInjection() {
    assertTrue(injector.getInstance(OVCTableHandle.class) instanceof HyperSQLOVCTableHandle);
  }
}
