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

package co.cask.cdap.data2.increment.hbase94;

import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseOrderedTable;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MockRegionServerServices;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class IncrementSummingScannerTest {
  private static final byte[] TRUE = Bytes.toBytes(true);
  private static HBaseTestingUtility testUtil;
  private static Configuration conf;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    testUtil = new HBaseTestingUtility();
    testUtil.startMiniCluster();
    conf = testUtil.getConfiguration();
  }

  @AfterClass
  public static void shutdownAfterClass() throws Exception {
    testUtil.shutdownMiniCluster();
  }

  @Test
  public void testIncrementScanning() throws Exception {
    String tableName = "TestIncrementSummingScanner";
    byte[] familyBytes = Bytes.toBytes("f");
    byte[] columnBytes = Bytes.toBytes("c");
    HRegion region = createRegion(tableName, familyBytes);
    try {
      region.initialize();

      // test handling of a single increment value alone
      Put p = new Put(Bytes.toBytes("r1"));
      p.add(familyBytes, columnBytes, Bytes.toBytes(3L));
      p.setAttribute(HBaseOrderedTable.DELTA_WRITE, TRUE);
      doPut(region, p);

      verifyCounts(region, new Scan(), new long[] {3L});

      // test handling of multiple increment values
      long now = System.currentTimeMillis();
      p = new Put(Bytes.toBytes("r3"));
      for (int i = 0; i < 5; i++) {
        p.add(familyBytes, columnBytes, now - i, Bytes.toBytes((long) (i + 1)));
      }
      p.setAttribute(HBaseOrderedTable.DELTA_WRITE, TRUE);
      doPut(region, p);

      verifyCounts(region, new Scan(Bytes.toBytes("r3")).setMaxVersions(), new long[] {15L});

      // test having single delta to sum with one of the multiple returned values
      // (r1 and r3 in this case are returned, but there's single delta increment to sum in r1)
      verifyCounts(region, new Scan().setMaxVersions(), new long[] {3L, 15L});


      // test handling of multiple increment values followed by a total sum, then other increments
      now = System.currentTimeMillis();
      p = new Put(Bytes.toBytes("r4"));
      for (int i = 0; i < 3; i++) {
        p.add(familyBytes, columnBytes, now - i, Bytes.toBytes(1L));
      }
      p.setAttribute(HBaseOrderedTable.DELTA_WRITE, TRUE);
      doPut(region, p);

      // this put will appear as a delta prior to all the delta puts
      p = new Put(Bytes.toBytes("r4"));
      p.add(familyBytes, columnBytes, now - 5, Bytes.toBytes(5L));
      p.setAttribute(HBaseOrderedTable.DELTA_WRITE, TRUE);
      doPut(region, p);

      verifyCounts(region, new Scan(Bytes.toBytes("r4")).setMaxVersions(), new long[] {8L});

      // test whatever we added so far
      verifyCounts(region, new Scan().setMaxVersions(), new long[] {3L, 15L, 8L});

      // test handling of an increment column followed by a non-increment column
      p = new Put(Bytes.toBytes("r4"));
      p.add(familyBytes, Bytes.toBytes("c2"), Bytes.toBytes("value"));
      doPut(region, p);

      Scan scan = new Scan(Bytes.toBytes("r4")).setMaxVersions();
      RegionScanner scanner = new IncrementSummingScanner(region, -1, region.getScanner(scan));
      List<KeyValue> results = Lists.newArrayList();
      scanner.next(results);

      assertEquals(2, results.size());
      KeyValue cell = results.get(0);
      assertNotNull(cell);
      assertEquals(8L, Bytes.toLong(cell.getValue()));

      cell = results.get(1);
      assertNotNull(cell);
      assertEquals("value", Bytes.toString(cell.getValue()));
    } finally {
      region.close();
    }

  }

  @Test
  public void testIncrementScanningWithLimit() throws Exception {
    String tableName = "TestIncrementSummingScannerWithLimit";
    byte[] familyBytes = Bytes.toBytes("f");
    byte[] columnBytes = Bytes.toBytes("c");
    HRegion region = createRegion(tableName, familyBytes);
    try {
      region.initialize();

      long now = System.currentTimeMillis();
      // adding 5 delta increments into each of two rows
      for (int i = 0; i < 5; i++) {
        Put p = new Put(Bytes.toBytes("r1"), now + i);
        p.add(familyBytes, columnBytes, Bytes.toBytes(1L));
        p.setAttribute(HBaseOrderedTable.DELTA_WRITE, TRUE);
        doPut(region, p);
      }

      // Verify scans with different batch limit work. At least these cases we want to cover:
      // * batch=<not set> // unlimited by default
      // * batch=1
      // * batch size less than delta inc group size
      // * batch size greater than delta inc group size
      // * batch size is bigger than all delta incs available
      for (int i = 0; i < 7; i++) {
//        verifyCounts(region, new Scan().setMaxVersions(), new long[] {5L}, i > 0 ? i : -1);
      }

      // Now test same with two groups of increments
      for (int i = 0; i < 5; i++) {
        Put p = new Put(Bytes.toBytes("r2"), now + 5 + i);
        p.add(familyBytes, columnBytes, Bytes.toBytes(2L));
        p.setAttribute(HBaseOrderedTable.DELTA_WRITE, TRUE);
        doPut(region, p);
      }

      verifyCounts(region, new Scan().setMaxVersions(), new long[] {5L, 10L}, 1);

      for (int i = 0; i < 12; i++) {
        System.out.println("BBBATCH: " + i);
        verifyCounts(region, new Scan().setMaxVersions(), new long[] {5L, 10L}, i > 0 ? i : -1);
      }
    } finally {
      region.close();
    }
  }

  private void verifyCounts(HRegion region, Scan scan, long[] counts) throws Exception {
    verifyCounts(region, scan, counts, -1);
  }

  private void verifyCounts(HRegion region, Scan scan, long[] counts, int batch) throws Exception {
    RegionScanner scanner = new IncrementSummingScanner(region, batch, region.getScanner(scan));
    // init with false if loop will execute zero times
    boolean hasMore = counts.length > 0;
    for (long count : counts) {
      List<KeyValue> results = Lists.newArrayList();
      hasMore = scanner.next(results);
      assertEquals(1, results.size());
      KeyValue cell = results.get(0);
      assertNotNull(cell);
      assertEquals(count, Bytes.toLong(cell.getValue()));
    }
    assertFalse(hasMore);
  }

  @Test
  public void testFlushAndCompact() throws Exception {
    String tableName = "TestFlushAndCompact";
    byte[] familyBytes = Bytes.toBytes("f");
    byte[] columnBytes = Bytes.toBytes("c");
    HRegion region = createRegion(tableName, familyBytes);
    try {
      region.initialize();

      // load an initial set of increments
      long ts = System.currentTimeMillis();
      byte[] row1 = Bytes.toBytes("row1");
      for (int i = 0; i < 50; i++) {
        Put p = new Put(row1);
        p.add(familyBytes, columnBytes, ts, Bytes.toBytes(1L));
        p.setAttribute(HBaseOrderedTable.DELTA_WRITE, TRUE);
        ts++;
        doPut(region, p);
      }

      byte[] row2 = Bytes.toBytes("row2");
      ts = System.currentTimeMillis();

      for (int i = 0; i < 10; i++) {
        Put p = new Put(row2);
        p.add(familyBytes, columnBytes, ts++, Bytes.toBytes(1L));
        p.setAttribute(HBaseOrderedTable.DELTA_WRITE, TRUE);
        doPut(region, p);
      }

      // force a region flush
      region.flushcache();
      region.waitForFlushesAndCompactions();

      Result r1 = region.get(new Get(row1));
      assertNotNull(r1);
      assertFalse(r1.isEmpty());
      // row1 should have a full put aggregating all 50 incrments
      KeyValue r1Cell = r1.getColumnLatest(familyBytes, columnBytes);
      assertNotNull(r1Cell);
      assertEquals(50L, Bytes.toLong(r1Cell.getValue()));

      Result r2 = region.get(new Get(row2));
      assertNotNull(r2);
      assertFalse(r2.isEmpty());
      // row2 should have a full put aggregating prior put + 10 increments
      KeyValue r2Cell = r2.getColumnLatest(familyBytes, columnBytes);
      assertNotNull(r2Cell);
      assertEquals(10L, Bytes.toLong(r2Cell.getValue()));
    } finally {
      region.close();
    }
  }

  private HRegion createRegion(String tableName, byte[] family) throws Exception {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor cfd = new HColumnDescriptor(family);
    cfd.setMaxVersions(Integer.MAX_VALUE);
    htd.addFamily(cfd);
    htd.addCoprocessor(IncrementHandler.class.getName());
    Path tablePath = new Path("/tmp/" + tableName);
    Path hlogPath = new Path("/tmp/hlog-" + tableName);
    Path oldPath = new Path("/tmp/.oldLogs-" + tableName);
    Configuration hConf = conf;
    FileSystem fs = FileSystem.get(hConf);
    assertTrue(fs.mkdirs(tablePath));
    HLog hlog = new HLog(fs, hlogPath, oldPath, hConf);
    return new HRegion(tablePath, hlog, fs, hConf,
                       new HRegionInfo(Bytes.toBytes(tableName)), htd, new MockRegionServerServices());
  }

  /**
   * Work around a bug in HRegion.internalPut(), where RegionObserver.prePut() modifications are not applied.
   */
  private void doPut(HRegion region, Put p) throws Exception {
    region.batchMutate(new Pair[]{ new Pair<Mutation, Integer>(p, null) });
  }
}
