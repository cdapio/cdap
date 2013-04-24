package com.continuuity.data.operation.executor.remote;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.hypersql.HyperSQLOVCTableHandle;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricLocalModule;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

import static org.junit.Assert.assertTrue;

public class HyperSQLOpexServiceTest extends OperationExecutorServiceTest {

  static Injector injector;

  @BeforeClass
  public static void startService() throws Exception {
    injector = Guice.createInjector (
        new DataFabricLocalModule("jdbc:hsqldb:mem:membenchdb", null));
    OperationExecutorServiceTest.startService(
        CConfiguration.create(), injector.getInstance(OperationExecutor.class));
  }

  @Override
  public void testInjection() {
    assertTrue(injector.getInstance(OVCTableHandle.class) instanceof HyperSQLOVCTableHandle);
  }

}
