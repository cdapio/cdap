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

package co.cask.cdap.data2.dataset2.lib.table.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.OrderedTable;
import co.cask.cdap.api.dataset.table.Tables;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.hbase.HBaseTestBase;
import co.cask.cdap.data.hbase.HBaseTestFactory;
import co.cask.cdap.data2.dataset2.lib.table.ordered.BufferingOrderedTable;
import co.cask.cdap.data2.dataset2.lib.table.ordered.BufferingOrderedTableTest;
import co.cask.cdap.data2.increment.hbase96.IncrementHandler;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.test.SlowTests;
import co.cask.tephra.Transaction;
import co.cask.tephra.inmemory.DetachedTxSystemClient;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 */
@Category(SlowTests.class)
public class HBaseOrderedTableTest extends BufferingOrderedTableTest<BufferingOrderedTable> {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseOrderedTableTest.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static HBaseTestBase testHBase;
  private static HBaseTableUtil hBaseTableUtil = new HBaseTableUtilFactory().get();

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
  protected BufferingOrderedTable getTable(String name, ConflictDetection conflictLevel) throws Exception {
    // ttl=-1 means "keep data forever"
    return
      new HBaseOrderedTable(name, ConflictDetection.valueOf(conflictLevel.name()), testHBase.getConfiguration(), true);
  }

  @Override
  protected HBaseOrderedTableAdmin getTableAdmin(String name, DatasetProperties props) throws IOException {
    DatasetSpecification spec = new HBaseOrderedTableDefinition("foo").configure(name, props);
    return new HBaseOrderedTableAdmin(spec, testHBase.getConfiguration(), hBaseTableUtil,
                                      CConfiguration.create(), new LocalLocationFactory(tmpFolder.newFolder()));
  }

  @Test
  public void testTTL() throws Exception {
    // for the purpose of this test it is fine not to configure ttl when creating table: we want to see if it
    // applies on reading
    int ttl = 1000;
    DatasetProperties props = DatasetProperties.builder().add(OrderedTable.PROPERTY_TTL, String.valueOf(ttl)).build();
    getTableAdmin("ttl", props).create();
    HBaseOrderedTable table = new HBaseOrderedTable("ttl", ConflictDetection.ROW, testHBase.getConfiguration(), false);

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
    byte[] val = table.get(b("row1"), b("col1"));
    if (val != null) {
      LOG.info("Unexpected value " + Bytes.toStringBinary(val));
    }
    Assert.assertNull(val);
    Assert.assertArrayEquals(b("val2"), table.get(b("row2"), b("col2")));

    // test a table with no TTL
    DatasetProperties props2 = DatasetProperties.builder()
      .add(OrderedTable.PROPERTY_TTL, String.valueOf(Tables.NO_TTL)).build();
    getTableAdmin("nottl", props2).create();
    HBaseOrderedTable table2 = new HBaseOrderedTable("nottl", ConflictDetection.ROW, testHBase.getConfiguration(),
                                                     false);

    tx = txSystemClient.startShort();
    table2.startTx(tx);
    table2.put(b("row1"), b("col1"), b("val1"));
    table2.commitTx();

    TimeUnit.SECONDS.sleep(2);

    tx = txSystemClient.startShort();
    table2.startTx(tx);
    table2.put(b("row2"), b("col2"), b("val2"));
    table2.commitTx();

    // if ttl is -1 (unlimited), it should see both
    tx = txSystemClient.startShort();
    table2.startTx(tx);
    Assert.assertArrayEquals(b("val1"), table2.get(b("row1"), b("col1")));
    Assert.assertArrayEquals(b("val2"), table2.get(b("row2"), b("col2")));
  }

  @Test
  public void testPreSplit() throws Exception {
    byte[][] splits = new byte[][] {Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c")};
    DatasetProperties props = DatasetProperties.builder().add("hbase.splits", new Gson().toJson(splits)).build();
    getTableAdmin("presplitted", props).create();

    HBaseAdmin hBaseAdmin = testHBase.getHBaseAdmin();
    try {
      List<HRegionInfo> regions = hBaseAdmin.getTableRegions(Bytes.toBytes("presplitted"));
      // note: first region starts at very first row key, so we have one extra to the splits count
      Assert.assertEquals(4, regions.size());
      Assert.assertArrayEquals(Bytes.toBytes("a"), regions.get(1).getStartKey());
      Assert.assertArrayEquals(Bytes.toBytes("b"), regions.get(2).getStartKey());
      Assert.assertArrayEquals(Bytes.toBytes("c"), regions.get(3).getStartKey());
    } finally {
      hBaseAdmin.close();
    }
  }

  @Test
  public void testEnableIncrements() throws Exception {
    // setup a table with increments disabled and with it enabled
    String disableTableName = "incr-disable";
    String enabledTableName = "incr-enable";
    HBaseOrderedTableAdmin disabledAdmin = getTableAdmin(disableTableName, DatasetProperties.EMPTY);
    disabledAdmin.create();

    DatasetProperties props =
      DatasetProperties.builder().add(OrderedTable.PROPERTY_READLESS_INCREMENT, "true").build();
    HBaseOrderedTableAdmin enabledAdmin = getTableAdmin(enabledTableName, props);
    enabledAdmin.create();

    try {
      HBaseAdmin admin = testHBase.getHBaseAdmin();
      try {
        HTableDescriptor htd = admin.getTableDescriptor(Bytes.toBytes(disableTableName));
        List<String> cps = htd.getCoprocessors();
        assertFalse(cps.contains(IncrementHandler.class.getName()));

        htd = admin.getTableDescriptor(Bytes.toBytes(enabledTableName));
        cps = htd.getCoprocessors();
        assertTrue(cps.contains(IncrementHandler.class.getName()));
      } finally {
        admin.close();
      }

      BufferingOrderedTable table = getTable(enabledTableName, ConflictDetection.COLUMN);
      byte[] row = Bytes.toBytes("row1");
      byte[] col = Bytes.toBytes("col1");
      DetachedTxSystemClient txSystemClient = new DetachedTxSystemClient();
      Transaction tx = txSystemClient.startShort();
      table.startTx(tx);
      table.increment(row, col, 10);
      table.commitTx();
      // verify that value was written as a delta value
      final byte[] expectedValue = Bytes.add(IncrementHandler.DELTA_MAGIC_PREFIX, Bytes.toBytes(10L));
      final AtomicBoolean foundValue = new AtomicBoolean();
      testHBase.forEachRegion(Bytes.toBytes(enabledTableName), new Function<HRegion, Object>() {
        @Nullable
        @Override
        public Object apply(@Nullable HRegion hRegion) {
          Scan scan = new Scan();
          try {
            RegionScanner scanner = hRegion.getScanner(scan);
            List<Cell> results = Lists.newArrayList();
            boolean hasMore;
            do {
              hasMore = scanner.next(results);
              for (Cell cell : results) {
                if (CellUtil.matchingValue(cell, expectedValue)) {
                  foundValue.set(true);
                }
              }
            } while (hasMore);
          } catch (IOException ioe) {
            fail("IOException scanning region: " + ioe.getMessage());
          }
          return null;
        }
      });
      assertTrue("Should have seen the expected encoded delta value in the " + enabledTableName + " table region",
                 foundValue.get());
    } finally {
      disabledAdmin.drop();
      enabledAdmin.drop();
    }
  }

  private static byte[] b(String s) {
    return Bytes.toBytes(s);
  }

}
