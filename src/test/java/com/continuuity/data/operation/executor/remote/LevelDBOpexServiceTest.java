package com.continuuity.data.operation.executor.remote;

import org.junit.BeforeClass;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class LevelDBOpexServiceTest extends OperationExecutorServiceTest {

  @BeforeClass
  public static void startService() throws Exception {
    Injector injector = Guice.createInjector (
        new DataFabricLevelDBModule());
    OperationExecutorServiceTest.startService(
        CConfiguration.create(), injector.getInstance(OperationExecutor.class));
  }

}
