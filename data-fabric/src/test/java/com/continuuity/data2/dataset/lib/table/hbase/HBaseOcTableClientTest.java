package com.continuuity.data2.dataset.lib.table.hbase;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.BufferingOcTableClientTest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;


/**
 *
 */
public class HBaseOcTableClientTest extends BufferingOcTableClientTest<HBaseOcTableClient> {

  static CConfiguration conf = CConfiguration.create();

  @BeforeClass
  public static void beforeClass() throws Exception{
    HBaseTestBase.startHBase();
    conf.set(HBaseTableUtil.CFG_TABLE_PREFIX, "test");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HBaseTestBase.stopHBase();
  }

  @Override
  protected HBaseOcTableClient getTable(String name) throws Exception {
    HBaseOcTableClient client = new HBaseOcTableClient(name, conf , HBaseTestBase.getConfiguration());
    Assert.assertTrue(client.getHBaseTableName().startsWith("test_"));
    return client;
  }

  @Override
  protected DataSetManager getTableManager() throws Exception {
    return new HBaseOcTableManager(conf, HBaseTestBase.getConfiguration());
  }
}
