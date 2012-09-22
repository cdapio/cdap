package com.continuuity.data.metadata;

import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data.runtime.DataFabricLocalModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class HBaseMetaDataStoreTest extends MetaDataStoreTest {

  @BeforeClass
  public static void setupOpex() throws Exception {
    Injector injector = Guice.createInjector (
        new DataFabricLocalModule("jdbc:hsqldb:mem:membenchdb", null));
    opex = injector.getInstance(OperationExecutor.class);
  }

  @BeforeClass
  public static void startService() throws Exception {
    HBaseTestBase.startHBase();
    DataFabricDistributedModule module =
        new DataFabricDistributedModule(HBaseTestBase.getConfiguration());
    Injector injector = Guice.createInjector(module);
    opex = injector.getInstance(Key.get(
        OperationExecutor.class, Names.named("DataFabricOperationExecutor")));
  }

  @AfterClass
  public static void stopHBase() throws Exception {
    HBaseTestBase.stopHBase();
  }

}
