package com.continuuity.data.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

/**
 * Tests for the tx guice module.
 */
public class DataFabricOpexModuleTest {

  @Test
  public void testGuiceInjector() throws Exception {
    DataFabricOpexModule module = new DataFabricOpexModule(CConfiguration.create(), HBaseConfiguration.create());
    CConfiguration configuration = module.getConfiguration();
    configuration.setBoolean(Constants.Transaction.Manager.CFG_DO_PERSIST, false);

    Injector injector = Guice.createInjector(new LocationRuntimeModule().getDistributedModules(),
                                             module);

    // get one tx manager
    InMemoryTransactionManager txManager1 = injector.getInstance(InMemoryTransactionManager.class);
    // get a second tx manager
    InMemoryTransactionManager txManager2 = injector.getInstance(InMemoryTransactionManager.class);
    // these should be two separate instances
    assertFalse(txManager1 == txManager2);
  }
}
