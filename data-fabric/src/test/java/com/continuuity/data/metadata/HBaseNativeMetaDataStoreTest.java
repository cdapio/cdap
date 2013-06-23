package com.continuuity.data.metadata;

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
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.junit.Assert.assertTrue;

public abstract class HBaseNativeMetaDataStoreTest extends MetaDataStoreTest {

  private static Injector injector;

  @BeforeClass
  public static void setupOpex() throws Exception {
    HBaseTestBase.startHBase();
    CConfiguration conf = CConfiguration.create();
    conf.setBoolean(DataFabricDistributedModule.CONF_ENABLE_NATIVE_QUEUES, true);
    DataFabricDistributedModule module = new DataFabricDistributedModule(HBaseTestBase.getConfiguration(),conf);
    injector = Guice.createInjector(module);
    opex = injector.getInstance(Key.get(OperationExecutor.class, Names.named("DataFabricOperationExecutor")));
    mds = new SerializingMetaDataStore(opex);
  }

  @AfterClass
  public static void stopHBase() throws Exception {
    HBaseTestBase.stopHBase();
  }

  @Override
  public void testInjection() {
    assertTrue(injector.getInstance(OVCTableHandle.class) instanceof HBaseNativeOVCTableHandle);
  }
}
