/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.util.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.data.hbase.HBaseTestBase;
import co.cask.cdap.data.hbase.HBaseTestFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public abstract class AbstractHBaseTableUtilTest {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractHBaseTableUtilTest.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static HBaseTestBase testHBase;
  private static HBaseAdmin hAdmin;

  @BeforeClass
  public static void beforeClass() throws Exception {
    testHBase = new HBaseTestFactory().get();
    testHBase.startHBase();
    hAdmin = new HBaseAdmin(testHBase.getConfiguration());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    hAdmin.close();
    testHBase.stopHBase();
  }

  protected abstract HBaseTableUtil getTableUtil();

  @Test
  public void testTableSizeMetrics() throws Exception {
    Assert.assertNull(getTableStats("table1"));
    Assert.assertNull(getTableStats("table2"));
    Assert.assertNull(getTableStats("table3"));

    create("table1");
    create("table2");
    create("table3");

    waitForMetricsToUpdate();

    Assert.assertEquals(0, getTableStats("table1").getTotalSizeMB());
    Assert.assertEquals(0, getTableStats("table2").getTotalSizeMB());
    Assert.assertEquals(0, getTableStats("table3").getTotalSizeMB());

    writeSome("table2");
    writeSome("table3");

    waitForMetricsToUpdate();

    Assert.assertEquals(0, getTableStats("table1").getTotalSizeMB());
    Assert.assertTrue(getTableStats("table2").getTotalSizeMB() > 0);
    Assert.assertTrue(getTableStats("table3").getTotalSizeMB() > 0);

    drop("table1");
    testHBase.forceRegionFlush(Bytes.toBytes("table2"));
    truncate("table3");

    waitForMetricsToUpdate();

    Assert.assertNull(getTableStats("table1"));
    Assert.assertTrue(getTableStats("table2").getTotalSizeMB() > 0);
    Assert.assertTrue(getTableStats("table2").getStoreFileSizeMB() > 0);
    Assert.assertEquals(0, getTableStats("table3").getTotalSizeMB());
  }

  private void waitForMetricsToUpdate() throws InterruptedException {
    // Wait for a bit to allow changes reflect in metrics: metrics updated on master with the heartbeat which
    // by default happens ~every 3 sec
    LOG.info("Waiting for metrics to reflect changes");
    TimeUnit.SECONDS.sleep(4);
  }

  private void writeSome(String tableName) throws IOException {
    HTable table = new HTable(testHBase.getConfiguration(), tableName);
    try {
      // writing at least couple megs to reflect in "megabyte"-based metrics
      for (int i = 0; i < 8; i++) {
        Put put = new Put(Bytes.toBytes("row" + i));
        put.add(Bytes.toBytes("d"), Bytes.toBytes("col" + i), new byte[1024 * 1024]);
        table.put(put);
      }
    } finally {
      table.close();
    }
  }

  private void create(String tableName) throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("d"));
    getTableUtil().createTableIfNotExists(hAdmin, tableName, desc);
  }

  private void truncate(String tableName) throws IOException {
    HTableDescriptor tableDescriptor = hAdmin.getTableDescriptor(Bytes.toBytes(tableName));
    hAdmin.disableTable(tableName);
    hAdmin.deleteTable(tableName);
    hAdmin.createTable(tableDescriptor);
  }

  private void drop(String tableName) throws IOException {
    hAdmin.disableTable(tableName);
    hAdmin.deleteTable(tableName);
  }

  private HBaseTableUtil.TableStats getTableStats(String tableName) throws IOException {
    return getTableUtil().getTableStats(hAdmin).get(tableName);
  }
}
