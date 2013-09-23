package com.continuuity.data2.dataset.lib.table.hbase;

import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.BufferingOcTableClientTest;
import com.continuuity.data2.dataset.lib.table.ConflictDetection;
import org.junit.AfterClass;
import org.junit.BeforeClass;


/**
 *
 */
public class HBaseOcTableClientTest extends BufferingOcTableClientTest<HBaseOcTableClient> {
  @BeforeClass
  public static void beforeClass() throws Exception{
    HBaseTestBase.startHBase();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HBaseTestBase.stopHBase();
  }

  @Override
  protected HBaseOcTableClient getTable(String name, ConflictDetection level) throws Exception {
    return new HBaseOcTableClient(name, level, HBaseTestBase.getConfiguration());
  }

  @Override
  protected DataSetManager getTableManager() throws Exception {
    return new HBaseOcTableManager(HBaseTestBase.getConfiguration());
  }
}
