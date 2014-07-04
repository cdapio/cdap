package com.continuuity.data2.dataset.lib.table.inmemory;

import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetsModules;
import com.continuuity.data2.dataset.lib.table.MetricsTableTest;
import com.continuuity.data2.transaction.runtime.TransactionMetricsModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

/**
 * test in-memory metrics tables.
 */
public class InMemoryMetricsTableTest extends MetricsTableTest {

  @BeforeClass
  public static void setup() {
    Injector injector = Guice.createInjector(new LocationRuntimeModule().getInMemoryModules(),
                                             new DiscoveryRuntimeModule().getInMemoryModules(),
                                             new DataFabricModules().getInMemoryModules(),
                                             new DataSetsModules().getInMemoryModule(),
                                             new TransactionMetricsModule());
    dsAccessor = injector.getInstance(DataSetAccessor.class);
  }
}
