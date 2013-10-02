package com.continuuity.data2.dataset.lib.table.hbase;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data2.dataset.lib.table.MetricsTableTest;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * metrics table test for levelDB.
 */
public class HBaseMetricsTableTest extends MetricsTableTest {

  @BeforeClass
  public static void setup() throws Exception {
    HBaseTestBase.startHBase();
    CConfiguration conf = CConfiguration.create();
    conf.unset(Constants.CFG_HDFS_USER);
    conf.setBoolean(Constants.Transaction.DataJanitor.CFG_TX_JANITOR_ENABLE, false);
    Injector injector = Guice.createInjector(new DataFabricDistributedModule(conf, HBaseTestBase.getConfiguration()),
                                             new ConfigModule(conf, HBaseTestBase.getConfiguration()),
                                             new LocationRuntimeModule().getDistributedModules());
    dsAccessor = injector.getInstance(DataSetAccessor.class);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    HBaseTestBase.stopHBase();
  }

}
