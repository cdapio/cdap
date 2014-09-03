/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.data2.metrics;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.data.hbase.HBaseTestBase;
import co.cask.cdap.data.hbase.HBaseTestFactory;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.test.XSlowTests;
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
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
@Category(XSlowTests.class)
public class HBaseDatasetStatsReporterTest {
  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static HBaseTestBase testHBase;
  private static HBaseTableUtil hBaseTableUtil = new HBaseTableUtilFactory().get();
  private static HBaseAdmin hAdmin;

  @BeforeClass
  public static void beforeClass() throws Exception {
    testHBase = new HBaseTestFactory().get();
    testHBase.startHBase(false);
    hAdmin = new HBaseAdmin(testHBase.getConfiguration());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    hAdmin.close();
    testHBase.stopHBase();
  }

  @Test
  public void testTableSizeMetrics() throws Exception {
    // Ideally we'd test how table changes are reflected in metrics, but that would require waiting on exposed by HBase
    // metrics to refresh, which currently takes 60 sec in unit-tests. If we wait couple times - that would be minutes,
    // so we don't do that here and checking metrics once, after some table & data manipulation.

    // 1. doing table & data manipulation.

    // Create couple tables:
    // - one we will keep empty
    // - second we will write some data in it
    // - third we'll write some data to it, but then truncate it
    //
    // In the end, first and third should have same size metric after flush, which is smaller than the second size.
    create("table1");
    create("table2");
    create("table3");

    writeSome("table2");
    writeSome("table3");

    truncate("table3");

    testHBase.forceRegionFlush(Bytes.toBytes("table1"));
    testHBase.forceRegionFlush(Bytes.toBytes("table2"));
    testHBase.forceRegionFlush(Bytes.toBytes("table3"));

    // 2. verifying metrics

    // todo: figure out how to make metrics update more frequently. Tried a lot, but still no luck
    TimeUnit.SECONDS.sleep(65);

    Assert.assertNull(getTableStats("noTable"));
    int table1Size = getTableStats("table1").getTotalSize();
    int table2Size = getTableStats("table2").getTotalSize();
    int table3Size = getTableStats("table3").getTotalSize();

    Assert.assertTrue(table1Size < table2Size);
    Assert.assertTrue(table3Size < table2Size);
    Assert.assertEquals(table1Size, table3Size);
  }

  private void writeSome(String tableName) throws IOException {
    HTable table = new HTable(testHBase.getConfiguration(), tableName);
    try {
      for (int i = 0; i < 100; i++) {
        Put put = new Put(Bytes.toBytes("row" + i));
        put.add(Bytes.toBytes("d"), Bytes.toBytes("col" + i), new byte[1024]);
        table.put(put);
      }
    } finally {
      table.close();
    }
  }

  private static void create(String tableName) throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("d"));
    hBaseTableUtil.createTableIfNotExists(hAdmin, tableName, desc);
  }

  private static void truncate(String tableName) throws IOException {
    HTableDescriptor tableDescriptor = hAdmin.getTableDescriptor(Bytes.toBytes(tableName));
    hAdmin.disableTable(tableName);
    hAdmin.deleteTable(tableName);
    hAdmin.createTable(tableDescriptor);
  }

  private static HBaseDatasetStatsReporter.TableStats getTableStats(String tableName) throws IOException {
    return HBaseDatasetStatsReporter.getTableStats(testHBase.getConfiguration()).get(tableName);
  }
}
