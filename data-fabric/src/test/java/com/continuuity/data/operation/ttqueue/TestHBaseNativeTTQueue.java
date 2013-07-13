package com.continuuity.data.operation.ttqueue;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.hbase.HBaseNativeOVCTableHandle;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Guice;
import org.junit.BeforeClass;

import static org.junit.Assert.assertTrue;

/**
 * test queues with native hbase transaction support.
 */
public class TestHBaseNativeTTQueue extends TestHBaseAbstractTTQueue {

  @Override
  public void testInjection() {
    assertTrue(handle instanceof HBaseNativeOVCTableHandle);
  }

  @BeforeClass
  public static void startEmbeddedHBase() {
    try {
      HBaseTestBase.startHBase();
      CConfiguration conf = CConfiguration.create();
      conf.setBoolean(DataFabricDistributedModule.CONFIG_ENABLE_NATIVE_HBASE, true);
      injector = Guice.createInjector(new DataFabricDistributedModule(HBaseTestBase.getConfiguration(), conf));
      handle = injector.getInstance(OVCTableHandle.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
