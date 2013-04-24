package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.hbase.HBaseNativeOVCTableHandle;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class NativeHBaseOpexServiceTest extends OperationExecutorServiceTest {

  static Injector injector;

  @BeforeClass
  public static void startService() throws Exception {
    HBaseTestBase.startHBase();
    Configuration hbaseConf = HBaseTestBase.getConfiguration();
    CConfiguration conf = CConfiguration.create();
    conf.setBoolean(
        DataFabricDistributedModule.CONF_ENABLE_NATIVE_QUEUES, true);
    DataFabricDistributedModule module =
        new DataFabricDistributedModule(hbaseConf, conf);
    injector = Guice.createInjector(module);
    OperationExecutorServiceTest.startService(module.getConfiguration(), injector.getInstance(Key.get
      (OperationExecutor.class, Names.named("DataFabricOperationExecutor"))));
  }

  @AfterClass
  public static void stopHBase() throws Exception {
    HBaseTestBase.stopHBase();
  }

  @Override
  public void testInjection() {
    assertTrue(injector.getInstance(OVCTableHandle.class) instanceof HBaseNativeOVCTableHandle);
  }

  // we must ignore this test for native HBase - it implements increment() incorrectly, and the test depends on it
  @Override @Test @Ignore
  public void testClientSideTransactions() throws OperationException {  }
}
