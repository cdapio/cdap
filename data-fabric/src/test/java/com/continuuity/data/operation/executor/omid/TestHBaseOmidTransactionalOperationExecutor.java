package com.continuuity.data.operation.executor.omid;

import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.engine.hbase.HBaseOVCTableHandle;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.data2.transaction.inmemory.StatePersistor;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.junit.Assert.assertTrue;

/**
 *
 */
public class  TestHBaseOmidTransactionalOperationExecutor  extends TestOmidTransactionalOperationExecutor {

  private static Injector injector;
  private static OmidTransactionalOperationExecutor executor;

  @BeforeClass
  public static void startEmbeddedHBase() {
    try {
      HBaseTestBase.startHBase();
      DataFabricDistributedModule module = new DataFabricDistributedModule(HBaseTestBase.getConfiguration());
      module.getConfiguration().setBoolean(StatePersistor.CFG_DO_PERSIST, false);
      injector = Guice.createInjector(module,
                                      new ConfigModule(module.getConfiguration(), HBaseTestBase.getConfiguration()),
                                      new LocationRuntimeModule().getInMemoryModules());
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

  @Override
  public void testInjection() {
    assertTrue(injector.getInstance(OVCTableHandle.class) instanceof HBaseOVCTableHandle);
  }
}
