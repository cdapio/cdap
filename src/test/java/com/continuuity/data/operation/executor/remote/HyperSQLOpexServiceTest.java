package com.continuuity.data.operation.executor.remote;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricLocalModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

public class HyperSQLOpexServiceTest extends OperationExecutorServiceTest {

  @BeforeClass
  public static void startService() throws Exception {
    CConfiguration config = CConfiguration.create();
    config.addResource("continuuity-data-fabric.xml");

    Injector injector = Guice.createInjector (
        new DataFabricLocalModule("jdbc:hsqldb:mem:membenchdb", null));
    OperationExecutorServiceTest.startService(
        config, injector.getInstance(OperationExecutor.class));
  }

}
