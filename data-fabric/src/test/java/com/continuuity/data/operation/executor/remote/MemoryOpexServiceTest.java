package com.continuuity.data.operation.executor.remote;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.memory.MemoryOVCTableHandle;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

import static org.junit.Assert.assertTrue;

public class MemoryOpexServiceTest extends OperationExecutorServiceTest {

  static Injector injector;

  @BeforeClass
  public static void startService() throws Exception {
    CConfiguration config = CConfiguration.create();
    injector = Guice.createInjector (
        new DataFabricModules().getInMemoryModules());
    OperationExecutorServiceTest.startService(
        config, injector.getInstance(OperationExecutor.class));
  }

  @Override
  public void testInjection() {
    assertTrue(injector.getInstance(OVCTableHandle.class) instanceof MemoryOVCTableHandle);
  }

}
