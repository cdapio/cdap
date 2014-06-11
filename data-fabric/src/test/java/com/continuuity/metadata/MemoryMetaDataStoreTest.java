package com.continuuity.metadata;

import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.NoOpMetricsCollectionService;
import com.continuuity.data.runtime.DataFabricModules;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
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
      new AbstractModule() {
      @Override
      protected void configure() {
        bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
      }
    });
  }
}
