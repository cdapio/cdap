package com.continuuity.data.metadata;

import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.LocationRuntimeModule;
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

/**
 * HBase meta data store tests.
 */
public abstract class HBaseMetaDataStoreTest extends MetaDataStoreTest {

  private static Injector injector;

  @BeforeClass
  public static void setupOpex() throws Exception {
    HBaseTestBase.startHBase();
    DataFabricDistributedModule module = new DataFabricDistributedModule(HBaseTestBase.getConfiguration());
    injector = Guice.createInjector(module,
                                    new ConfigModule(module.getConfiguration(), HBaseTestBase.getConfiguration()),
                                    new LocationRuntimeModule().getInMemoryModules());
    opex = injector.getInstance(Key.get(
        OperationExecutor.class, Names.named("DataFabricOperationExecutor")));
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
