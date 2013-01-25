package com.continuuity.data.metadata;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class HBaseSerializingMetaDataStoreTest extends HBaseMetaDataStoreTest {

  @BeforeClass
  public static void setupOpex() throws Exception {
    HBaseTestBase.startHBase();
    CConfiguration conf = CConfiguration.create();
    conf.setBoolean(DataFabricDistributedModule.CONF_ENABLE_NATIVE_QUEUES, false);
    DataFabricDistributedModule module = new DataFabricDistributedModule(HBaseTestBase.getConfiguration(),conf);
    Injector injector = Guice.createInjector(module);
    opex = injector.getInstance(Key.get(OperationExecutor.class, Names.named("DataFabricOperationExecutor")));
    mds = new SerializingMetaDataStore(opex);
  }

  @AfterClass
  public static void stopHBase() throws Exception {
    HBaseTestBase.stopHBase();
  }

  // Tests that do not work on Vanilla HBase

  @Override @Test @Ignore
  public void testConcurrentSwapField() throws Exception {  }

  /**
   * Currently not working.  Will be fixed in ENG-1840.
   */
  @Override @Test @Ignore
  public void testConcurrentUpdate() throws Exception {  }
}