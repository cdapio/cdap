package com.continuuity.data.operation.executor.remote;

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

public class HBaseOpexServiceTest extends OperationExecutorServiceTest {

  @BeforeClass
  public static void startService() throws Exception {
    HBaseTestBase.startHBase();
    Injector injector = Guice.createInjector(
        new DataFabricDistributedModule(HBaseTestBase.getConfiguration()));
    OperationExecutorServiceTest.startService(
        injector.getInstance(Key.get(
            OperationExecutor.class,
            Names.named("DataFabricOperationExecutor"))));
  }

  @AfterClass
  public static void stopHBase() throws Exception {
    HBaseTestBase.stopHBase();
  }

  @Override @Ignore @Test
  public void testWriteBatchThenReadAllKeys() throws Exception { }

  @Override @Ignore @Test
  public void testClearFabric() { }

}
