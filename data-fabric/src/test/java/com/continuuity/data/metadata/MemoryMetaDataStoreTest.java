package com.continuuity.data.metadata;

import com.continuuity.data.runtime.DataFabricModules;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

/**
 * Memory metadata store tests.
 */
public abstract class MemoryMetaDataStoreTest extends MetaDataStoreTest {

  protected static Injector injector;

  @BeforeClass
  public static void setupDataFabric() throws Exception {

    injector = Guice.createInjector (
        new DataFabricModules().getInMemoryModules());
  }
}
