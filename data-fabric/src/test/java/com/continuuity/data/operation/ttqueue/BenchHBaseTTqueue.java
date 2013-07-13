package com.continuuity.data.operation.ttqueue;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Guice;
import org.junit.BeforeClass;

/**
 *
 */
public class BenchHBaseTTqueue extends BenchAbstractHBaseTTqueue {

  @BeforeClass
  public static void startEmbeddedHBase() {
    try {
      HBaseTestBase.startHBase();
      CConfiguration conf = CConfiguration.create();
      conf.setBoolean(DataFabricDistributedModule.CONFIG_ENABLE_NATIVE_HBASE, false);
      injector = Guice.createInjector(new DataFabricDistributedModule(HBaseTestBase.getConfiguration(), conf));
      handle = injector.getInstance(OVCTableHandle.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
