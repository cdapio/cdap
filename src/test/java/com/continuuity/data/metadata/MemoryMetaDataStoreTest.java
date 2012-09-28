package com.continuuity.data.metadata;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

public abstract class MemoryMetaDataStoreTest extends MetaDataStoreTest {

  @BeforeClass
  public static void setupOpex() throws Exception {

    CConfiguration config = CConfiguration.create();
    Injector injector = Guice.createInjector (
        new DataFabricModules().getInMemoryModules());
    opex = injector.getInstance(OperationExecutor.class);
  }

}
