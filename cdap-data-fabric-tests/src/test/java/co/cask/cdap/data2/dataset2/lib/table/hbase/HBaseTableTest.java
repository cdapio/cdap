/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.dataset.table.Tables;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.hbase.HBaseTestBase;
import co.cask.cdap.data.hbase.HBaseTestFactory;
import co.cask.cdap.data2.dataset2.lib.table.BufferingTable;
import co.cask.cdap.data2.dataset2.lib.table.BufferingTableTest;
import co.cask.cdap.data2.increment.hbase.IncrementHandlerState;
import co.cask.cdap.data2.increment.hbase96.IncrementHandler;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.test.SlowTests;
import co.cask.tephra.DefaultTransactionExecutor;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.inmemory.DetachedTxSystemClient;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
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
public class HBaseTableTest extends BufferingTableTest<BufferingTable> {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseTableTest.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static HBaseTestBase testHBase;
  private static HBaseTableUtil hBaseTableUtil;
  private static CConfiguration cConf;

  @BeforeClass
  public static void beforeClass() throws Exception {
    testHBase = new HBaseTestFactory().get();
    testHBase.startHBase();
    cConf = CConfiguration.create();
    hBaseTableUtil = new HBaseTableUtilFactory(cConf).get();
    // TODO: CDAP-1634 - Explore a way to not have every HBase test class do this.
    hBaseTableUtil.createNamespaceIfNotExists(testHBase.getHBaseAdmin(), NAMESPACE_ID);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    hBaseTableUtil.deleteAllInNamespace(testHBase.getHBaseAdmin(), NAMESPACE_ID);
    hBaseTableUtil.deleteNamespaceIfExists(testHBase.getHBaseAdmin(), NAMESPACE_ID);
    testHBase.stopHBase();
  }

  @Override
  protected BufferingTable getTable(String name, ConflictDetection conflictLevel) throws Exception {
    // ttl=-1 means "keep data forever"
    DatasetSpecification spec = DatasetSpecification.builder(name, "foo")
      .property(Table.PROPERTY_READLESS_INCREMENT, "true")
      .property(Table.PROPERTY_CONFLICT_LEVEL, conflictLevel.name())
      .build();
    return new HBaseTable(spec, testHBase.getConfiguration(), hBaseTableUtil);
  }

  @Override
  protected HBaseTableAdmin getTableAdmin(String name, DatasetProperties props) throws IOException {
    DatasetSpecification spec = new HBaseTableDefinition("foo").configure(name, props);
    return new HBaseTableAdmin(spec, testHBase.getConfiguration(), hBaseTableUtil,
                               cConf, new LocalLocationFactory(tmpFolder.newFolder()));
  }

  @Test
  public void testTTL() throws Exception {
    // for the purpose of this test it is fine not to configure ttl when creating table: we want to see if it
    // applies on reading
    int ttl = 1000;
    String ttlTable = DS_NAMESPACE.namespace(NAMESPACE_ID, "ttl");
    String noTtlTable = DS_NAMESPACE.namespace(NAMESPACE_ID, "nottl");
    DatasetProperties props = DatasetProperties.builder().add(Table.PROPERTY_TTL, String.valueOf(ttl)).build();
    getTableAdmin(ttlTable, props).create();

    DatasetSpecification ttlTableSpec = DatasetSpecification.builder(ttlTable, HBaseTable.class.getName())
      .properties(props.getProperties())
      .build();

    HBaseTable table = new HBaseTable(ttlTableSpec, testHBase.getConfiguration(), hBaseTableUtil);

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
      .add(Table.PROPERTY_TTL, String.valueOf(Tables.NO_TTL)).build();
    getTableAdmin(noTtlTable, props2).create();

    DatasetSpecification noTtlTableSpec = DatasetSpecification.builder(noTtlTable, HBaseTable.class.getName())
      .properties(props2.getProperties())
      .build();
    HBaseTable table2 = new HBaseTable(noTtlTableSpec, testHBase.getConfiguration(), hBaseTableUtil);

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
    String presplittedTable = DS_NAMESPACE.namespace(NAMESPACE_ID, "presplitted");
    getTableAdmin(presplittedTable, props).create();

    HBaseAdmin hBaseAdmin = testHBase.getHBaseAdmin();
    try {
      List<HRegionInfo> regions = hBaseTableUtil.getTableRegions(hBaseAdmin, TableId.from(presplittedTable));
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
    String disableTableName = DS_NAMESPACE.namespace(NAMESPACE_ID, "incr-disable");
    String enabledTableName = DS_NAMESPACE.namespace(NAMESPACE_ID, "incr-enable");
    TableId disabledTableId = TableId.from(disableTableName);
    TableId enabledTableId = TableId.from(enabledTableName);
    HBaseTableAdmin disabledAdmin = getTableAdmin(disableTableName, DatasetProperties.EMPTY);
    disabledAdmin.create();
    HBaseAdmin admin = testHBase.getHBaseAdmin();

    DatasetProperties props =
      DatasetProperties.builder().add(Table.PROPERTY_READLESS_INCREMENT, "true").build();
    HBaseTableAdmin enabledAdmin = getTableAdmin(enabledTableName, props);
    enabledAdmin.create();

    try {

      try {
        HTableDescriptor htd = hBaseTableUtil.getHTableDescriptor(admin, disabledTableId);
        List<String> cps = htd.getCoprocessors();
        assertFalse(cps.contains(IncrementHandler.class.getName()));

        htd = hBaseTableUtil.getHTableDescriptor(admin, enabledTableId);
        cps = htd.getCoprocessors();
        assertTrue(cps.contains(IncrementHandler.class.getName()));
      } finally {
        admin.close();
      }

      BufferingTable table = getTable(enabledTableName, ConflictDetection.COLUMN);
      byte[] row = Bytes.toBytes("row1");
      byte[] col = Bytes.toBytes("col1");
      DetachedTxSystemClient txSystemClient = new DetachedTxSystemClient();
      Transaction tx = txSystemClient.startShort();
      table.startTx(tx);
      table.increment(row, col, 10);
      table.commitTx();
      // verify that value was written as a delta value
      final byte[] expectedValue = Bytes.add(IncrementHandlerState.DELTA_MAGIC_PREFIX, Bytes.toBytes(10L));
      final AtomicBoolean foundValue = new AtomicBoolean();
      byte [] enabledTableNameBytes = hBaseTableUtil.getHTableDescriptor(admin, enabledTableId).getName();
      testHBase.forEachRegion(enabledTableNameBytes, new Function<HRegion, Object>() {
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

  @Test
  public void testColumnFamily() throws Exception {
    DatasetProperties props = DatasetProperties.builder().add(Table.PROPERTY_COLUMN_FAMILY, "t").build();
    HBaseTableDefinition tableDefinition = new HBaseTableDefinition("foo");
    String tableName = DS_NAMESPACE.namespace(NAMESPACE_ID, "testcf");
    DatasetSpecification spec = tableDefinition.configure(tableName, props);

    DatasetAdmin admin = new HBaseTableAdmin(spec, testHBase.getConfiguration(), hBaseTableUtil,
                                             CConfiguration.create(), new LocalLocationFactory(tmpFolder.newFolder()));
    admin.create();
    final HBaseTable table = new HBaseTable(spec, testHBase.getConfiguration(), hBaseTableUtil);

    TransactionSystemClient txClient = new DetachedTxSystemClient();
    TransactionExecutor executor = new DefaultTransactionExecutor(txClient, table);
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        table.put(new Put("row", "column", "testValue"));
      }
    });

    final HBaseTable table2 = new HBaseTable(spec, testHBase.getConfiguration(), hBaseTableUtil);
    executor = new DefaultTransactionExecutor(txClient, table2);
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals("testValue", table2.get(new Get("row", "column")).getString("column"));
      }
    });

    // Verify the column family name
    HTableDescriptor htd = hBaseTableUtil.getHTableDescriptor(testHBase.getHBaseAdmin(), TableId.from(tableName));
    HColumnDescriptor hcd = htd.getFamily(Bytes.toBytes("t"));
    Assert.assertNotNull(hcd);
    Assert.assertEquals("t", hcd.getNameAsString());
  }

  private static byte[] b(String s) {
    return Bytes.toBytes(s);
  }

}
