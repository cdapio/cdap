package com.continuuity.data2.dataset.lib.table.hbase;

import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTableTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 *
 */
public class HBaseOcTableClientTest extends OrderedColumnarTableTest {
  @BeforeClass
  public static void beforeClass() throws Exception{
    HBaseTestBase.startHBase();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HBaseTestBase.stopHBase();
  }

  @Override
  protected OrderedColumnarTable getTable(String name) throws Exception {
    return new HBaseOcTableClient(name, HBaseTestBase.getConfiguration());
  }

  @Override
  protected DataSetManager getTableManager() throws Exception {
    return new HBaseOcTableManager(HBaseTestBase.getConfiguration());
  }
}
