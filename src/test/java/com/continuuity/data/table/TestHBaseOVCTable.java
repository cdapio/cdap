package com.continuuity.data.table;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestHBaseOVCTable extends TestOVCTable {
  
  private static Injector injector;

  @BeforeClass
  public static void startEmbeddedHBase() {
    try {
      HBaseTestBase.startHBase();
      injector = Guice.createInjector(
          new DataFabricDistributedModule(HBaseTestBase.getConfiguration()));
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
  protected OVCTableHandle getTableHandle() {
    return injector.getInstance(OVCTableHandle.class);
  }

  // Tests that do not work on HBase
  
  @Override @Test @Ignore
  public void testFormatVerySimply() {}
  
  // These tests don't pass yet for hbase

//  @Override @Test @Ignore
//  public void testSameVersionOverwritesExisting() {}
//
//  @Override @Test @Ignore
//  public void testDeleteBehavior() {}
//
//  @Override @Test @Ignore
//  public void testGetAllKeys() {}
}
