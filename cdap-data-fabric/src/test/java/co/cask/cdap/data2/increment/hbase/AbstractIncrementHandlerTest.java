/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.increment.hbase;

import co.cask.cdap.data.hbase.HBaseTestBase;
import co.cask.cdap.data.hbase.HBaseTestFactory;
import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseOrderedTable;
import co.cask.tephra.TxConstants;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Common test cases for HBase version-specific {@code IncrementHandlerTest} implementations.
 */
public abstract class AbstractIncrementHandlerTest {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractIncrementHandlerTest.class);

  protected static final byte[] EMPTY_BYTES = new byte[0];
  protected static final byte[] FAMILY = Bytes.toBytes("i");

  protected static HBaseTestBase testUtil;
  protected static Configuration conf;

  protected long ts = 1;

  @BeforeClass
  public static void setup() throws Exception {
    testUtil = new HBaseTestFactory().get();
    testUtil.startHBase();
    conf = testUtil.getConfiguration();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    testUtil.stopHBase();
  }

  @Test
  public void testIncrements() throws Exception {
    String tableName = "incrementTest";
    createTable("incrementTest");

    HTable table = new HTable(conf, tableName);
    try {
      byte[] colA = Bytes.toBytes("a");
      byte[] row1 = Bytes.toBytes("row1");

      // test column containing only increments
      table.put(newIncrement(row1, colA, 1));
      table.put(newIncrement(row1, colA, 1));
      table.put(newIncrement(row1, colA, 1));

      assertColumn(table, row1, colA, 3);

      // test intermixed increments and puts
      Put putA = new Put(row1);
      putA.add(FAMILY, colA, ts++, Bytes.toBytes(5L));
      table.put(putA);

      assertColumn(table, row1, colA, 5);

      table.put(newIncrement(row1, colA, 1));
      table.put(newIncrement(row1, colA, 1));

      assertColumn(table, row1, colA, 7);

      // test multiple increment columns
      byte[] row2 = Bytes.toBytes("row2");
      byte[] colB = Bytes.toBytes("b");

      // increment A and B twice at the same timestamp
      table.put(newIncrement(row2, colA, 1, 1));
      table.put(newIncrement(row2, colB, 1, 1));
      table.put(newIncrement(row2, colA, 2, 1));
      table.put(newIncrement(row2, colB, 2, 1));
      // increment A once more
      table.put(newIncrement(row2, colA, 1));

      assertColumns(table, row2, new byte[][]{ colA, colB }, new long[]{ 3, 2 });

      // overwrite B with a new put
      Put p = new Put(row2);
      p.add(FAMILY, colB, ts++, Bytes.toBytes(10L));
      table.put(p);

      assertColumns(table, row2, new byte[][]{ colA, colB }, new long[]{ 3, 10 });
    } finally {
      table.close();
    }
  }

  @Test
  public void testIncrementsCompaction() throws Exception {
    // In this test we verify that squashing delta-increments during flush or compaction works as designed.

    String tableName = "incrementCompactTest";
    byte[] tableBytes = Bytes.toBytes(tableName);
    createTable(tableName);

    HTable table = new HTable(conf, tableName);
    try {
      byte[] colA = Bytes.toBytes("a");
      byte[] row1 = Bytes.toBytes("row1");

      // do some increments
      table.put(newIncrement(row1, colA, 1));
      table.put(newIncrement(row1, colA, 1));
      table.put(newIncrement(row1, colA, 1));

      assertColumn(table, row1, colA, 3);

      testUtil.forceRegionFlush(tableBytes);

      // verify increments after flush
      assertColumn(table, row1, colA, 3);

      // do some more increments
      table.put(newIncrement(row1, colA, 1));
      table.put(newIncrement(row1, colA, 1));
      table.put(newIncrement(row1, colA, 1));

      // verify increments merged well from hstore and memstore
      assertColumn(table, row1, colA, 6);

      testUtil.forceRegionFlush(tableBytes);

      // verify increments merged well into hstores
      assertColumn(table, row1, colA, 6);

      // do another iteration to verify that multiple "squashed" increments merged well at scan and at flush
      table.put(newIncrement(row1, colA, 1));
      table.put(newIncrement(row1, colA, 1));
      table.put(newIncrement(row1, colA, 1));

      assertColumn(table, row1, colA, 9);
      testUtil.forceRegionFlush(tableBytes);
      assertColumn(table, row1, colA, 9);

      // verify increments merged well on minor compaction
      testUtil.forceRegionCompact(tableBytes, false);
      assertColumn(table, row1, colA, 9);

      // another round of increments to verify that merged on compaction merges well with memstore and with new hstores
      table.put(newIncrement(row1, colA, 1));
      table.put(newIncrement(row1, colA, 1));
      table.put(newIncrement(row1, colA, 1));

      assertColumn(table, row1, colA, 12);
      testUtil.forceRegionFlush(tableBytes);
      assertColumn(table, row1, colA, 12);

      // do same, but with major compaction
      // verify increments merged well on minor compaction
      testUtil.forceRegionCompact(tableBytes, true);
      assertColumn(table, row1, colA, 12);

      // another round of increments to verify that merged on compaction merges well with memstore and with new hstores
      table.put(newIncrement(row1, colA, 1));
      table.put(newIncrement(row1, colA, 1));
      table.put(newIncrement(row1, colA, 1));

      assertColumn(table, row1, colA, 15);
      testUtil.forceRegionFlush(tableBytes);
      assertColumn(table, row1, colA, 15);

    } finally {
      table.close();
    }
  }

  @Test
  public void testIncrementsCompactionUnlimBound() throws Exception {
    RegionWrapper region = createRegion("testIncrementsCompactionsUnlimBound", ImmutableMap.<String, String>builder()
        .put(IncrementHandlerState.PROPERTY_TRANSACTIONAL, "false").build());

    try {
      region.initialize();

      byte[] colA = Bytes.toBytes("a");
      byte[] row1 = Bytes.toBytes("row1");

      // do some increments
      region.put(newIncrement(row1, colA, 1));
      region.put(newIncrement(row1, colA, 1));
      region.put(newIncrement(row1, colA, 1));

      region.flush();

      // verify increments merged after flush
      assertSingleVersionColumn(region, row1, colA, 3);

      // do some more increments
      region.put(newIncrement(row1, colA, 1));
      region.put(newIncrement(row1, colA, 1));
      region.put(newIncrement(row1, colA, 1));

      region.flush();
      region.compact(true);

      // verify increments merged well into hstores
      assertSingleVersionColumn(region, row1, colA, 6);
    } finally {
      region.close();
    }
  }

  @Test
  public void testNonTransactionalMixed() throws Exception {
    // test mix of increment, put and delete operations

    String tableName = "testNonTransactionalMixed";
    createTable(tableName);

    byte[] row1 = Bytes.toBytes("r1");
    byte[] col = Bytes.toBytes("c");
    HTable table = new HTable(conf, tableName);
    try {
      // perform 100 increments on a column
      for (int i = 0; i < 100; i++) {
        table.put(newIncrement(row1, col, 1));
      }

      assertColumn(table, row1, col, 100);

      // do a new put on the column
      Put put = new Put(row1);
      put.add(FAMILY, col, Bytes.toBytes(11L));
      table.put(put);

      assertColumn(table, row1, col, 11);

      // perform a delete on the column
      Delete delete = new Delete(row1);
      delete.deleteColumns(FAMILY, col);
      // use batch to work around a bug in delete coprocessor hooks on HBase 0.94
      table.batch(Lists.newArrayList(delete));

      Get get = new Get(row1);
      Result result = table.get(get);
      LOG.info("Get after delete returned " + result);
      assertTrue(result.isEmpty());

      // perform 100 increments on a column
      for (int i = 0; i < 100; i++) {
        table.put(newIncrement(row1, col, 1));
      }

      assertColumn(table, row1, col, 100);

      // perform a family delete
      delete = new Delete(row1);
      delete.deleteFamily(FAMILY);
      // use batch to work around a bug in delete coprocessor hooks on HBase 0.94
      table.batch(Lists.newArrayList(delete));

      get = new Get(row1);
      result = table.get(get);
      LOG.info("Get after delete returned " + result);
      assertTrue(result.isEmpty());

      // do 100 more increments
      for (int i = 0; i < 100; i++) {
        table.put(newIncrement(row1, col, 1));
      }

      assertColumn(table, row1, col, 100);

      // perform a row delete
      delete = new Delete(row1);
      // use batch to work around a bug in delete coprocessor hooks on HBase 0.94
      table.batch(Lists.newArrayList(delete));

      get = new Get(row1);
      result = table.get(get);
      LOG.info("Get after delete returned " + result);
      assertTrue(result.isEmpty());

      // do 100 more increments
      for (int i = 0; i < 100; i++) {
        table.put(newIncrement(row1, col, 1));
      }

      assertColumn(table, row1, col, 100);
    } finally {
      table.close();
    }
  }

  /**
   * Verifies that time-to-live based expiration of data is applied correctly when the {@code IncrementHandler}
   * coprocessor is generating timestamps, ie. for non-transactional writes.  TTL-based expiration of increment
   * values follows a couple of rule:
   * <ol>
   *   <li>delta writes (increments) are never TTL'd</li>
   *   <li>a normal put is only TTL'd if not preceeded by newer increments in the same column</li>
   * </ol>
   */
  @Test
  public void testNonTransactionalTTL() throws Exception {
    RegionWrapper region = createRegion("testNonTransactionalTTL", ImmutableMap.<String, String>builder()
        .put(IncrementHandlerState.PROPERTY_TRANSACTIONAL, "false")
        .put(TxConstants.PROPERTY_TTL, "50").build());

    byte[] row = Bytes.toBytes("r1");
    byte[] col = Bytes.toBytes("c");
    try {
      region.initialize();

      SettableTimestampOracle timeOracle = new SettableTimestampOracle();
      region.setCoprocessorTimestampOracle(timeOracle);

      // test that we do not apply TTL in the middle of a set of increments
      long now = System.currentTimeMillis();

      for (int i = 100; i > 0; i--) {
        timeOracle.setCurrentTime((now - i) * IncrementHandlerState.MAX_TS_PER_MS);
        // timestamp will be overridden by IncrementHandler coprocessor
        region.put(newIncrement(row, col, Integer.MAX_VALUE, 1));
      }
      // reset "current time"
      timeOracle.setCurrentTime(now * IncrementHandlerState.MAX_TS_PER_MS);

      List<ColumnCell> results = Lists.newArrayList();
      assertFalse(region.scanRegion(results, row));
      // verify we have 100 individual cells, one per increment
      assertEquals(100, results.size());
      for (int i = 0; i < results.size(); i++) {
        ColumnCell cell = results.get(i);
        assertEquals(1L, Bytes.toLong(cell.getValue(), 2));
        Assert.assertEquals((now - i - 1) * IncrementHandlerState.MAX_TS_PER_MS, cell.getTimestamp());
      }

      // verify that when summing we return the correct sum

      // run a flush, verify that all cells are included in the summed value
      region.flush();

      results.clear();
      assertFalse(region.scanRegion(results, row));
      // verify 1 increment cell now exists
      assertEquals(1, results.size());
      assertEquals(100L, Bytes.toLong(results.get(0).getValue(), 2));
      // should have the timestamp from the most recent increment
      Assert.assertEquals((now - 1) * IncrementHandlerState.MAX_TS_PER_MS, results.get(0).getTimestamp());

      // test that we do not apply TTL to a put terminating a set of increments
      byte[] row2 = Bytes.toBytes("r2");
      // first add a full put
      Put p = new Put(row2);
      p.add(FAMILY, col, Bytes.toBytes(50L));
      region.put(p);

      // move 51 msec into the future, so that the previous put is behind the TTL
      now = now + 51;

      // add some increments
      for (int i = 10; i > 0; i--) {
        timeOracle.setCurrentTime((now - i) * IncrementHandlerState.MAX_TS_PER_MS);
        // timestamp will be overridden by IncrementHandler coprocessor
        region.put(newIncrement(row2, col, Integer.MAX_VALUE, 1));
      }
      // reset "current time"
      timeOracle.setCurrentTime(now * IncrementHandlerState.MAX_TS_PER_MS);

      results.clear();
      assertFalse(region.scanRegion(results, row2));
      assertEquals(11, results.size());
      // first 10 cells should be the increments
      for (int i = 0; i < 10; i++) {
        ColumnCell cell = results.get(i);
        Assert.assertEquals((now - i - 1) * IncrementHandlerState.MAX_TS_PER_MS, cell.getTimestamp());
        assertEquals(1L, Bytes.toLong(cell.getValue(), 2));
      }

      // last should be the full put
      ColumnCell cell = results.get(10);
      Assert.assertEquals((now - 51) * IncrementHandlerState.MAX_TS_PER_MS, cell.getTimestamp());
      assertEquals(50L, Bytes.toLong(cell.getValue()));

      // do a compaction
      region.flush();
      region.compact(true);

      // verify that the 10 increments and 1 put are summed to 1 cell (as a normal put)
      results.clear();
      assertFalse(region.scanRegion(results, row2));
      assertEquals(1, results.size());
      assertEquals(60L, Bytes.toLong(results.get(0).getValue()));
      Assert.assertEquals((now - 1) * IncrementHandlerState.MAX_TS_PER_MS, results.get(0).getTimestamp());

      // test that we apply TTL to a put preceeded by a non-TTL'd put
      // go 50 msec into the future
      now = now + 50;
      timeOracle.setCurrentTime(now * IncrementHandlerState.MAX_TS_PER_MS);

      // do another full put
      p = new Put(row2);
      p.add(FAMILY, col, Bytes.toBytes(99L));
      region.put(p);

      // run a compaction to apply TTL
      region.compact(true);
      // the previously summed value should now be gone
      results.clear();
      assertFalse(region.scanRegion(results, row2));
      assertEquals(1, results.size());
      assertEquals(99L, Bytes.toLong(results.get(0).getValue()));
      Assert.assertEquals(now * IncrementHandlerState.MAX_TS_PER_MS, results.get(0).getTimestamp());

      // test that we apply TTL to a standalone put
      byte[] row3 = Bytes.toBytes("r3");
      p = new Put(row3);
      p.add(FAMILY, col, Bytes.toBytes(11L));
      region.put(p);

      results.clear();
      assertFalse(region.scanRegion(results, row3));
      assertEquals(1, results.size());
      assertEquals(11L, Bytes.toLong(results.get(0).getValue()));

      // advance 51 msec into the future
      now = now + 51;
      timeOracle.setCurrentTime(now * IncrementHandlerState.MAX_TS_PER_MS);

      region.flush();
      region.compact(true);
      // the stand alone put should be gone
      results.clear();
      assertFalse(region.scanRegion(results, row3));
      assertEquals(0, results.size());
    } finally {
      region.close();
    }
  }

  public Put newIncrement(byte[] row, byte[] column, long value) {
      return newIncrement(row, column, ts++, value);
  }

  public Put newIncrement(byte[] row, byte[] column, long timestamp, long value) {
    Put p = new Put(row);
    p.add(FAMILY, column, timestamp, Bytes.toBytes(value));
    p.setAttribute(HBaseOrderedTable.DELTA_WRITE, EMPTY_BYTES);
    return p;
  }

  public abstract void assertColumn(HTable table, byte[] row, byte[] col, long expected) throws Exception;

  public void assertSingleVersionColumn(RegionWrapper region, byte[] row, byte[] col, long expected) throws Exception {
    List<ColumnCell> results = Lists.newArrayList();
    Assert.assertFalse(region.scanRegion(results, row, new byte[][]{col}));
    Assert.assertEquals(1, results.size());
    byte[] value = results.get(0).getValue();
    // note: it may be stored as increment delta even after merge on flush/compact
    long longValue =
        Bytes.toLong(value, value.length > Bytes.SIZEOF_LONG ? IncrementHandlerState.DELTA_MAGIC_PREFIX.length : 0);
    Assert.assertEquals(expected, longValue);
  }

  public abstract void assertColumns(HTable table, byte[] row, byte[][] cols, long[] expected) throws Exception;

  public abstract RegionWrapper createRegion(String tableName, Map<String, String> familyProperties) throws Exception;

  public abstract void createTable(String tableName) throws Exception;

  public static class ColumnCell {
    private final byte[] row;
    private final byte[] family;
    private final byte[] qualifier;
    private final long timestamp;
    private final byte[] value;

    public ColumnCell(byte[] row, byte[] family, byte[] qualifier, long timestamp, byte[] value) {
      this.row = row;
      this.family = family;
      this.qualifier = qualifier;
      this.timestamp = timestamp;
      this.value = value;
    }

    public byte[] getRow() {
      return row;
    }

    public byte[] getFamily() {
      return family;
    }

    public byte[] getQualifier() {
      return qualifier;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public byte[] getValue() {
      return value;
    }
  }

  public interface RegionWrapper extends Closeable {
    void initialize() throws IOException;

    void put(Put put) throws IOException;

    boolean scanRegion(List<ColumnCell> results, byte[] startRow) throws IOException;

    boolean scanRegion(List<ColumnCell> results, byte[] startRow, byte[][] column) throws IOException;

    boolean flush() throws IOException;

    void compact(boolean majorCompact) throws IOException;

    void setCoprocessorTimestampOracle(TimestampOracle timeOracle);
  }
}
