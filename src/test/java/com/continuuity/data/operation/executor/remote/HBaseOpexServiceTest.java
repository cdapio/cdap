package com.continuuity.data.operation.executor.remote;

import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class HBaseOpexServiceTest extends OperationExecutorServiceTest {

  @BeforeClass
  public static void startService() throws Exception {
    HBaseTestBase.startHBase();
    DataFabricDistributedModule module =
        new DataFabricDistributedModule(HBaseTestBase.getConfiguration());
    Injector injector = Guice.createInjector(module);
    OperationExecutorServiceTest.startService(
        module.getConfiguration(),
        injector.getInstance(Key.get(
            OperationExecutor.class,
            Names.named("DataFabricOperationExecutor"))));
  }

  @AfterClass
  public static void stopHBase() throws Exception {
    HBaseTestBase.stopHBase();
  }

}
