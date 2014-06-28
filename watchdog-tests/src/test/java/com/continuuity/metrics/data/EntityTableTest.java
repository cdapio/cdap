/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.hbase.HBaseTestFactory;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data2.dataset.lib.table.MetricsTable;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.transaction.runtime.TransactionMetricsModule;
import com.continuuity.test.SlowTests;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 *
 */
@Category(SlowTests.class)
public class EntityTableTest {

  private static DataSetAccessor accessor;
  private static HBaseTestBase testHBase;

  protected MetricsTable getTable(String name) throws Exception {
    accessor.getDataSetManager(MetricsTable.class, DataSetAccessor.Namespace.SYSTEM).create(name);
    return accessor.getDataSetClient(name, MetricsTable.class, DataSetAccessor.Namespace.SYSTEM);
  }

  @Test
  public void testGetId() throws Exception {
    EntityTable entityTable = new EntityTable(getTable("testGetId"));

    // Make sure it is created sequentially
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals((long) i, entityTable.getId("app", "app" + i));
    }

    // It should get the same value (from cache)
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals((long) i, entityTable.getId("app", "app" + i));
    }

    // Construct another entityTable, it should load from storage.
    entityTable = new EntityTable(getTable("testGetId"));
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals((long) i, entityTable.getId("app", "app" + i));
    }

    // ID for different type should have ID starts from 1 again.
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals((long) i, entityTable.getId("flow", "flow" + i));
    }
  }

  @Test
  public void testGetName() throws Exception {
    EntityTable entityTable = new EntityTable(getTable("testGetName"));

    // Create some entities.
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals((long) i, entityTable.getId("app", "app" + i));
    }

    // Reverse lookup
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals("app" + i, entityTable.getName(i, "app"));
    }
  }


  @BeforeClass
  public static void init() throws Exception {
    testHBase = new HBaseTestFactory().get();
    testHBase.startHBase();
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, testHBase.getZkConnectionString());
    cConf.unset(Constants.CFG_HDFS_USER);
    cConf.setBoolean(TxConstants.DataJanitor.CFG_TX_JANITOR_ENABLE, false);

    Injector injector = Guice.createInjector(new ConfigModule(cConf, testHBase.getConfiguration()),
                                             new DiscoveryRuntimeModule().getDistributedModules(),
                                             new ZKClientModule(),
                                             new DataFabricDistributedModule(),
                                             new LocationRuntimeModule().getDistributedModules(),
                                             new TransactionMetricsModule());

    accessor = injector.getInstance(DataSetAccessor.class);
  }

  @AfterClass
  public static void finish() throws Exception {
    testHBase.stopHBase();
  }

}
