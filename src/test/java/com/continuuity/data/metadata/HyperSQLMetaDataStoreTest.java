package com.continuuity.data.metadata;

import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricLocalModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

public abstract class HyperSQLMetaDataStoreTest extends MetaDataStoreTest {

  @BeforeClass
  public static void setupOpex() throws Exception {
    Injector injector = Guice.createInjector (
        new DataFabricLocalModule("jdbc:hsqldb:mem:membenchdb", null));
    opex = injector.getInstance(OperationExecutor.class);
    opex.execute(OperationContext.DEFAULT,
        new ClearFabric(ClearFabric.ToClear.ALL));
  }

}
