package com.continuuity.data;

import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetsModules;
import com.continuuity.data2.transaction.runtime.TransactionMetricsModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

/**
 *
 */
public class InMemoryDataSetAccessorTest extends NamespacingDataSetAccessorTest {
  private static DataSetAccessor dsAccessor;

  @BeforeClass
  public static void beforeClass() throws Exception {
    NamespacingDataSetAccessorTest.beforeClass();
    Injector injector = Guice.createInjector(
      new ConfigModule(conf),
      new DataFabricModules().getInMemoryModules(),
      new DataSetsModules().getInMemoryModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new LocationRuntimeModule().getInMemoryModules(),
      new TransactionMetricsModule()
    );
    dsAccessor = injector.getInstance(DataSetAccessor.class);
  }

  @Override
  protected DataSetAccessor getDataSetAccessor() {
    return dsAccessor;
  }
}
