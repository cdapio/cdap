package com.continuuity.data2.dataset.lib.table.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.hbase.HBaseTestFactory;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.BufferingOcTableClientTest;
import com.continuuity.data2.dataset.lib.table.ConflictDetection;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.transaction.inmemory.DetachedTxSystemClient;
import com.continuuity.data2.util.hbase.HBaseTableUtil;
import com.continuuity.data2.util.hbase.HBaseTableUtilFactory;
import com.continuuity.test.SlowTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.HDFSLocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.TimeUnit;


/**
 *
 */
@Category(SlowTests.class)
public class HBaseOcTableClientTest extends BufferingOcTableClientTest<HBaseOcTableClient> {
  private static HBaseTestBase testHBase;

  @BeforeClass
  public static void beforeClass() throws Exception {
    testHBase = new HBaseTestFactory().get();
    testHBase.startHBase();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    testHBase.stopHBase();
  }

  @Override
  protected HBaseOcTableClient getTable(String name, ConflictDetection level) throws Exception {
    return new HBaseOcTableClient(name, level, testHBase.getConfiguration());
  }

  @Override
  protected DataSetManager getTableManager() throws Exception {
    Configuration hConf = testHBase.getConfiguration();
    CConfiguration conf = CConfiguration.create();
    conf.unset(Constants.CFG_HDFS_USER);
    conf.setBoolean(TxConstants.DataJanitor.CFG_TX_JANITOR_ENABLE, false);
    HBaseTableUtil tableUtil = new HBaseTableUtilFactory().get();
    return new HBaseOcTableManager(conf, hConf, new HDFSLocationFactory(hConf), tableUtil);
  }

  @Test
  public void testTTL() throws Exception {
    // for the purpose of this test it is fine not to configure ttl when creating table: we want to see if it
    // applies on reading
    getTableManager().create("ttl");
    HBaseOcTableClient table = new HBaseOcTableClient("ttl", ConflictDetection.ROW, 1000, testHBase.getConfiguration());

    DetachedTxSystemClient txSystemClient = new DetachedTxSystemClient();
    Transaction tx = txSystemClient.startShort();
    table.startTx(tx);
    table.put(b("row1"), b("col1"), b("val1"));
    table.commitTx();

    TimeUnit.SECONDS.sleep(2);

    tx = txSystemClient.startShort();
    table.startTx(tx);
    table.put(b("row2"), b("col2"), b("val2"));
    table.commitTx();

    // now, we should not see first as it should have expired, but see the last one
    tx = txSystemClient.startShort();
    table.startTx(tx);
    Assert.assertNull(table.get(b("row1"), b("col1")));
    Assert.assertArrayEquals(b("val2"), table.get(b("row2"), b("col2")));

    // if ttl is 30 sec, it should see both
    table = new HBaseOcTableClient("ttl", ConflictDetection.ROW, 1000 * 30, testHBase.getConfiguration());
    tx = txSystemClient.startShort();
    table.startTx(tx);
    Assert.assertArrayEquals(b("val1"), table.get(b("row1"), b("col1")));
    Assert.assertArrayEquals(b("val2"), table.get(b("row2"), b("col2")));

    // if ttl is -1 (unlimited), it should see both
    table = new HBaseOcTableClient("ttl", ConflictDetection.ROW, -1, testHBase.getConfiguration());
    tx = txSystemClient.startShort();
    table.startTx(tx);
    Assert.assertArrayEquals(b("val1"), table.get(b("row1"), b("col1")));
    Assert.assertArrayEquals(b("val2"), table.get(b("row2"), b("col2")));
  }

  private static byte[] b(String s) {
    return Bytes.toBytes(s);
  }
}
