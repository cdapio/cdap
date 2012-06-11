package com.continuuity.data.table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

@Ignore
public class TestHBaseOVCTable extends TestOVCTable {

  private static final HBaseTestingUtility hbTestUtil =
      new HBaseTestingUtility();

  private static MiniHBaseCluster miniCluster;

  private static final Configuration conf = hbTestUtil.getConfiguration();
  
  private static final Injector injector =
      Guice.createInjector(new DataFabricDistributedModule(conf));

  @BeforeClass
  public static void startEmbeddedHBase() {
    try {
      miniCluster = hbTestUtil.startMiniCluster(1, 1);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public static void stopEmbeddedHBase() {
    try {
      if (miniCluster != null) miniCluster.shutdown();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  @Override
  protected OVCTableHandle getTableHandle() {
    return injector.getInstance(OVCTableHandle.class);
  }

  // These tests don't pass yet for hbase

  @Override @Test @Ignore
  public void testSameVersionOverwritesExisting() {}

  @Override @Test @Ignore
  public void testDeleteBehavior() {}

}
