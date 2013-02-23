package com.continuuity.data.metadata;

import org.junit.BeforeClass;

import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

public abstract class LevelDBMetaDataStoreTest extends MetaDataStoreTest {

  @BeforeClass
  public static void setupOpex() throws Exception {
    Injector injector = Guice.createInjector (
        new DataFabricLevelDBModule());
    opex = injector.getInstance(OperationExecutor.class);
    opex.execute(OperationContext.DEFAULT,
        new ClearFabric(ClearFabric.ToClear.ALL));
  }

}
