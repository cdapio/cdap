package com.continuuity.data.table;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.hbase.HBaseNativeOVCTable;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

public class  TestHBaseNativeOVCTable extends TestOVCTable {

  private static final Logger Log = LoggerFactory.getLogger(TestHBaseNativeOVCTable.class);
  protected static Injector injector;

  @BeforeClass
  public static void startEmbeddedHBase() {
    try {
      HBaseTestBase.startHBase();
      CConfiguration conf = CConfiguration.create();
      conf.setBoolean(DataFabricDistributedModule.CONF_ENABLE_NATIVE_QUEUES, true);
      injector = Guice.createInjector(new DataFabricDistributedModule(HBaseTestBase.getConfiguration(),conf));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public static void stopEmbeddedHBase() {
    try {
      HBaseTestBase.stopHBase();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected OVCTableHandle injectTableHandle() {
    return injector.getInstance(OVCTableHandle.class);
  }

  // Tests that do not work on HBase

  /**
   * Currently not working.  Will be fixed in ENG-421.
   */
  @Override @Test
  @Ignore
  public void testIncrementCASIncrementWithSameTimestamp() {}

  /**
   * Not working because native hbase ignores excludes in read pointer
   */
  @Override @Test
  @Ignore
  public void testIncrementIgnoresInProgressXactions() {}

  @Override
  public void testInjection() {
    assertTrue(table instanceof HBaseNativeOVCTable);
  }
}
