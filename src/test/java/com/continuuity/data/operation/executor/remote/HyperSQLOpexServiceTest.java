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
    Injector injector = Guice.createInjector (
        new DataFabricLocalModule("jdbc:hsqldb:mem:membenchdb", null));
    OperationExecutorServiceTest.startService(
        CConfiguration.create(), injector.getInstance(OperationExecutor.class));
  }

}
