package com.continuuity.data.operation.executor.remote;

import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

public class HyperSQLOpexServiceTest extends OperationExecutorServiceTest {

  @BeforeClass
  public static void startService() throws Exception {
    Injector injector = Guice.createInjector (
        new DataFabricModules().getSingleNodeModules());
    OperationExecutorServiceTest.startService(injector.
        getInstance(OperationExecutor.class));
  }

}
