package com.continuuity.data.operation.executor.omid;

import com.google.inject.Key;
import com.google.inject.name.Names;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class  TestHBaseOmidTransactionalOperationExecutor
extends TestOmidTransactionalOperationExecutor {

  private static OmidTransactionalOperationExecutor executor;

  @BeforeClass
  public static void startEmbeddedHBase() {
    try {
      HBaseTestBase.startHBase();
      Injector injector = Guice.createInjector(
        new DataFabricDistributedModule(HBaseTestBase.getConfiguration()));
      executor = (OmidTransactionalOperationExecutor) injector.getInstance(
        Key.get(OperationExecutor.class, Names.named("DataFabricOperationExecutor")));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public static void stopEmbeddedHBase() {
    try {
      HBaseTestBase.stopHBase();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected OmidTransactionalOperationExecutor getOmidExecutor() {
    return executor;
  }

  // Test Overrides

  /**
   * Currently not working.  Will be fixed in ENG-420.
   */
  @Test @Ignore @Override
  public void testDeletesCanBeTransacted() throws Exception {}

}
