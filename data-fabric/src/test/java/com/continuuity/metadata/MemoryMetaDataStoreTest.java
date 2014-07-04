package com.continuuity.metadata;

import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetsModules;
import com.continuuity.data2.transaction.runtime.TransactionMetricsModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

/**
 * Memory metadata store tests.
 */
public abstract class MemoryMetaDataStoreTest extends MetaDataTableTest {

  protected static Injector injector;

  @BeforeClass
  public static void setupDataFabric() throws Exception {

    injector = Guice.createInjector (
      new LocationRuntimeModule().getInMemoryModules(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new DataFabricModules().getInMemoryModules(),
      new DataSetsModules().getInMemoryModule(),
      new TransactionMetricsModule());
  }
}
