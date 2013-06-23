package com.continuuity.data.operation.executor.remote;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.hbase.HBaseOVCTableHandle;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.junit.Assert.assertTrue;

public class HBaseOpexServiceTest extends OperationExecutorServiceTest {

  static Injector injector;

  @BeforeClass
  public static void startService() throws Exception {
    HBaseTestBase.startHBase();
    CConfiguration conf = CConfiguration.create();
    // make sure we use vanilla hbase
    conf.setBoolean(DataFabricDistributedModule.CONF_ENABLE_NATIVE_QUEUES, false);
    DataFabricDistributedModule module = new DataFabricDistributedModule(HBaseTestBase.getConfiguration(), conf);
    injector = Guice.createInjector(module);
    OperationExecutorServiceTest.startService(
      module.getConfiguration(), injector.getInstance(Key.get(OperationExecutor.class,
                                                              Names.named ("DataFabricOperationExecutor"))));
  }

  @AfterClass
  public static void stopHBase() throws Exception {
    HBaseTestBase.stopHBase();
  }

  @Override
  public void testInjection() {
    assertTrue(injector.getInstance(OVCTableHandle.class) instanceof HBaseOVCTableHandle);
  }

}
