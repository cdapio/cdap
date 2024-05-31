/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.data2.increment.hbase10cdh550;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.data.hbase.HBase10CDH550Test;
import io.cdap.cdap.data2.dataset2.lib.table.hbase.HBaseTable;
import io.cdap.cdap.data2.increment.hbase.IncrementHandlerState;
import io.cdap.cdap.data2.util.TableId;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtil;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtilFactory;
import io.cdap.cdap.data2.util.hbase.HTableDescriptorBuilder;
import io.cdap.cdap.proto.id.NamespaceId;
import java.net.InetAddress;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MockRegionServerServices;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Tests for {@link IncrementSummingScanner} implementation.
 */
public class IncrementSummingScannerTest {

  @ClassRule
  public static final HBase10CDH550Test TEST_HBASE = new HBase10CDH550Test();

  private static final byte[] TRUE = Bytes.toBytes(true);
  private static Configuration conf;
  private static CConfiguration cConf;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    conf = TEST_HBASE.getConfiguration();
    cConf = CConfiguration.create();
  }

  @Test
  public void testIncrementScanning() throws Exception {
    TableId tableId = TableId.from(NamespaceId.DEFAULT.getNamespace(), "TestIncrementSummingScanner");
    byte[] familyBytes = Bytes.toBytes("f");
    byte[] columnBytes = Bytes.toBytes("c");
    HRegion region = createRegion(tableId, familyBytes);
    try {
      region.initialize();

      // test handling of a single increment value alone
      Put p = new Put(Bytes.toBytes("r1"));
      p.add(familyBytes, columnBytes, Bytes.toBytes(3L));
      p.setAttribute(HBaseTable.DELTA_WRITE, TRUE);
      region.put(p);

      Scan scan = new Scan();
      RegionScanner scanner = new IncrementSummingScanner(region, -1, region.getScanner(scan), ScanType.USER_SCAN);
      List<Cell> results = Lists.newArrayList();
      scanner.next(results);

      assertEquals(1, results.size());
      Cell cell = results.get(0);
      assertNotNull(cell);
      assertEquals(3L, Bytes.toLong(cell.getValue()));

      // test handling of a single total sum
      p = new Put(Bytes.toBytes("r2"));
      p.add(familyBytes, columnBytes, Bytes.toBytes(5L));
      region.put(p);

      scan = new Scan(Bytes.toBytes("r2"));

      scanner = new IncrementSummingScanner(region, -1, region.getScanner(scan), ScanType.USER_SCAN);
      results = Lists.newArrayList();
      scanner.next(results);

      assertEquals(1, results.size());
      cell = results.get(0);
      assertNotNull(cell);
      assertEquals(5L, Bytes.toLong(cell.getValue()));

      // test handling of multiple increment values
      long now = System.currentTimeMillis();
      p = new Put(Bytes.toBytes("r3"));
      for (int i = 0; i < 5; i++) {
        p.add(familyBytes, columnBytes, now - i, Bytes.toBytes((long) (i + 1)));
      }
      p.setAttribute(HBaseTable.DELTA_WRITE, TRUE);
      region.put(p);

      scan = new Scan(Bytes.toBytes("r3"));
      scan.setMaxVersions();
      scanner = new IncrementSummingScanner(region, -1, region.getScanner(scan), ScanType.USER_SCAN);
      results = Lists.newArrayList();
      scanner.next(results);

      assertEquals(1, results.size());
      cell = results.get(0);
      assertNotNull(cell);
      assertEquals(15L, Bytes.toLong(cell.getValue()));

      // test handling of multiple increment values followed by a total sum, then other increments
      now = System.currentTimeMillis();
      p = new Put(Bytes.toBytes("r4"));
      for (int i = 0; i < 3; i++) {
        p.add(familyBytes, columnBytes, now - i, Bytes.toBytes(1L));
      }
      p.setAttribute(HBaseTable.DELTA_WRITE, TRUE);
      region.put(p);

      // this put will appear as a "total" sum prior to all the delta puts
      p = new Put(Bytes.toBytes("r4"));
      p.add(familyBytes, columnBytes, now - 5, Bytes.toBytes(5L));
      region.put(p);

      scan = new Scan(Bytes.toBytes("r4"));
      scan.setMaxVersions();
      scanner = new IncrementSummingScanner(region, -1, region.getScanner(scan), ScanType.USER_SCAN);
      results = Lists.newArrayList();
      scanner.next(results);

      assertEquals(1, results.size());
      cell = results.get(0);
      assertNotNull(cell);
      assertEquals(8L, Bytes.toLong(cell.getValue()));

      // test handling of an increment column followed by a non-increment column
      p = new Put(Bytes.toBytes("r4"));
      p.add(familyBytes, Bytes.toBytes("c2"), Bytes.toBytes("value"));
      region.put(p);

      scan = new Scan(Bytes.toBytes("r4"));
      scan.setMaxVersions();
      scanner = new IncrementSummingScanner(region, -1, region.getScanner(scan), ScanType.USER_SCAN);
      results = Lists.newArrayList();
      scanner.next(results);

      assertEquals(2, results.size());
      cell = results.get(0);
      assertNotNull(cell);
      assertEquals(8L, Bytes.toLong(cell.getValue()));

      cell = results.get(1);
      assertNotNull(cell);
      assertEquals("value", Bytes.toString(cell.getValue()));

      // test handling of an increment column followed by a delete
      now = System.currentTimeMillis();
      Delete d = new Delete(Bytes.toBytes("r5"));
      d.deleteColumn(familyBytes, columnBytes, now - 3);
      region.delete(d);

      p = new Put(Bytes.toBytes("r5"));
      for (int i = 2; i >= 0; i--) {
        p.add(familyBytes, columnBytes, now - i, Bytes.toBytes(1L));
      }
      p.setAttribute(HBaseTable.DELTA_WRITE, TRUE);
      region.put(p);

      scan = new Scan(Bytes.toBytes("r5"));
      scan.setMaxVersions();
      scan.setRaw(true);
      scanner = new IncrementSummingScanner(region, -1, region.getScanner(scan), ScanType.COMPACT_RETAIN_DELETES);
      results = Lists.newArrayList();
      scanner.next(results);

      // delete marker will not be returned for user scan
      assertEquals(2, results.size());
      cell = results.get(0);
      assertNotNull(cell);
      assertEquals(3L, Bytes.toLong(cell.getValue(), IncrementHandlerState.DELTA_MAGIC_PREFIX.length, 8));
      // next cell should be the delete
      cell = results.get(1);
      assertTrue(CellUtil.isDelete(cell));
    } finally {
      region.close();
    }

  }

  @Test
  public void testFlushAndCompact() throws Exception {
    TableId tableId = TableId.from(NamespaceId.DEFAULT.getNamespace(), "TestFlushAndCompact");
    byte[] familyBytes = Bytes.toBytes("f");
    byte[] columnBytes = Bytes.toBytes("c");
    HRegion region = createRegion(tableId, familyBytes);
    try {
      region.initialize();

      // load an initial set of increments
      long ts = System.currentTimeMillis();
      byte[] row1 = Bytes.toBytes("row1");
      for (int i = 0; i < 50; i++) {
        Put p = new Put(row1);
        p.add(familyBytes, columnBytes, ts, Bytes.toBytes(1L));
        p.setAttribute(HBaseTable.DELTA_WRITE, TRUE);
        ts++;
        region.put(p);
      }

      byte[] row2 = Bytes.toBytes("row2");
      ts = System.currentTimeMillis();
      // start with a full put
      Put row2P = new Put(row2);
      row2P.add(familyBytes, columnBytes, ts++, Bytes.toBytes(10L));
      region.put(row2P);
      for (int i = 0; i < 10; i++) {
        Put p = new Put(row2);
        p.add(familyBytes, columnBytes, ts++, Bytes.toBytes(1L));
        p.setAttribute(HBaseTable.DELTA_WRITE, TRUE);
        region.put(p);
      }

      // force a region flush
      region.flushcache();
      region.waitForFlushesAndCompactions();

      Result r1 = region.get(new Get(row1));
      assertNotNull(r1);
      assertFalse(r1.isEmpty());
      // row1 should have a full put aggregating all 50 incrments
      Cell r1Cell = r1.getColumnLatestCell(familyBytes, columnBytes);
      assertNotNull(r1Cell);
      assertEquals(50L, Bytes.toLong(r1Cell.getValue()));

      Result r2 = region.get(new Get(row2));
      assertNotNull(r2);
      assertFalse(r2.isEmpty());
      // row2 should have a full put aggregating prior put + 10 increments
      Cell r2Cell = r2.getColumnLatestCell(familyBytes, columnBytes);
      assertNotNull(r2Cell);
      assertEquals(20L, Bytes.toLong(r2Cell.getValue()));

      // add 30 more increments to row2
      for (int i = 0; i < 30; i++) {
        Put p = new Put(row2);
        p.add(familyBytes, columnBytes, ts++, Bytes.toBytes(1L));
        p.setAttribute(HBaseTable.DELTA_WRITE, TRUE);
        region.put(p);
      }

      // row2 should now have a full put aggregating prior 20 value + 30 increments
      r2 = region.get(new Get(row2));
      assertNotNull(r2);
      assertFalse(r2.isEmpty());
      r2Cell = r2.getColumnLatestCell(familyBytes, columnBytes);
      assertNotNull(r2Cell);
      assertEquals(50L, Bytes.toLong(r2Cell.getValue()));

      // force another region flush
      region.flushcache();
      region.waitForFlushesAndCompactions();

      // add 100 more increments to row2
      for (int i = 0; i < 100; i++) {
        Put p = new Put(row2);
        p.add(familyBytes, columnBytes, ts++, Bytes.toBytes(1L));
        p.setAttribute(HBaseTable.DELTA_WRITE, TRUE);
        region.put(p);
      }

      // row2 should now have a full put aggregating prior 50 value + 100 increments
      r2 = region.get(new Get(row2));
      assertNotNull(r2);
      assertFalse(r2.isEmpty());
      r2Cell = r2.getColumnLatestCell(familyBytes, columnBytes);
      assertNotNull(r2Cell);
      assertEquals(150L, Bytes.toLong(r2Cell.getValue()));
    } finally {
      region.close();
    }
  }

  @Test
  public void testMultiColumnFlushAndCompact() throws Exception {
    TableId tableId = TableId.from(NamespaceId.DEFAULT.getNamespace(), "testMultiColumnFlushAndCompact");
    byte[] familyBytes = Bytes.toBytes("f");
    byte[] columnBytes = Bytes.toBytes("c");
    byte[] columnBytes2 = Bytes.toBytes("c2");
    HRegion region = createRegion(tableId, familyBytes);
    try {
      region.initialize();

      long now = 1;
      byte[] row1 = Bytes.toBytes("row1");
      byte[] row2 = Bytes.toBytes("row2");

      // Initial put to row1,c2
      Put row1P = new Put(row1);
      row1P.add(familyBytes, columnBytes2, now - 1, Bytes.toBytes(5L));
      region.put(row1P);

      // Initial put to row2,c
      Put row2P = new Put(row2);
      row2P.add(familyBytes, columnBytes, now - 1, Bytes.toBytes(10L));
      region.put(row2P);

      // Generate some increments
      long ts = now;
      for (int i = 0; i < 50; i++) {
        region.put(generateIncrementPut(familyBytes, columnBytes, row1, ts));
        region.put(generateIncrementPut(familyBytes, columnBytes, row2, ts));
        region.put(generateIncrementPut(familyBytes, columnBytes2, row1, ts));
        ts++;
      }

      // First scanner represents flush scanner
      RegionScanner scanner =
        new IncrementSummingScanner(region, -1, region.getScanner(new Scan().setMaxVersions()),
                                    ScanType.COMPACT_RETAIN_DELETES, now + 15, -1);
      // Second scanner is a user scan, this is to help in easy asserts
      scanner = new IncrementSummingScanner(region, -1, scanner, ScanType.USER_SCAN);

      List<Cell> results = Lists.newArrayList();
      assertTrue(scanner.next(results, ScannerContext.newBuilder().setBatchLimit(10).build()));
      assertEquals(2, results.size());
      Cell cell = results.get(0);
      assertNotNull(cell);
      assertEquals("row1", Bytes.toString(cell.getRow()));
      assertEquals("c", Bytes.toString(cell.getQualifier()));
      assertEquals(50, Bytes.toLong(cell.getValue()));

      cell = results.get(1);
      assertNotNull(cell);
      assertEquals("row1", Bytes.toString(cell.getRow()));
      assertEquals("c2", Bytes.toString(cell.getQualifier()));
      assertEquals(55, Bytes.toLong(cell.getValue()));

      results.clear();
      assertFalse(scanner.next(results, ScannerContext.newBuilder().setBatchLimit(10).build()));
      assertEquals(1, results.size());
      cell = results.get(0);
      assertNotNull(cell);
      assertEquals("row2", Bytes.toString(cell.getRow()));
      assertEquals(60, Bytes.toLong(cell.getValue()));
    } finally {
      region.close();
    }
  }

  @Test
  public void testIncrementScanningWithBatchAndUVB() throws Exception {
    TableId tableId = TableId.from(NamespaceId.DEFAULT.getNamespace(),
                                   "TestIncrementSummingScannerWithUpperVisibilityBound");
    byte[] familyBytes = Bytes.toBytes("f");
    byte[] columnBytes = Bytes.toBytes("c");
    HRegion region = createRegion(tableId, familyBytes);
    try {
      region.initialize();

      long start = 0;
      long now = start;
      long counter1 = 0;

      // adding 5 delta increments
      for (int i = 0; i < 5; i++) {
        Put p = new Put(Bytes.toBytes("r1"), now++);
        p.add(familyBytes, columnBytes, Bytes.toBytes(1L));
        p.setAttribute(HBaseTable.DELTA_WRITE, TRUE);
        region.put(p);
        counter1++;
      }

      // different combinations of uvb and limit (see batch test above)
      // At least these cases we want to cover for batch:
      // * batch=<not set> // unlimited by default
      // * batch=1
      // * batch size less than delta inc group size
      // * batch size greater than delta inc group size
      // * batch size is bigger than all delta incs available
      // At least these cases we want to cover for uvb:
      // * uvb=<not set> // 0
      // * uvb less than max tx of delta inc
      // * uvb greater than max tx of delta inc
      // * multiple uvbs applied to simulate multiple flush & compactions
      // Also: we want different combinations of batch limit & uvbs
      for (int i = 0; i < 7; i++) {
        for (int k = 0; k < 4; k++) {
          long[] uvbs = new long[k];
          for (int l = 0; l < uvbs.length; l++) {
            uvbs[l] = start + (k + 1) * (l + 1);
          }
          verifyCounts(region, new Scan().setMaxVersions(), new long[]{counter1}, i > 0 ? i : -1, uvbs);
        }
      }

      // Now test same with two groups of increments
      int counter2 = 0;
      for (int i = 0; i < 5; i++) {
        Put p = new Put(Bytes.toBytes("r2"), now + i);
        p.add(familyBytes, columnBytes, Bytes.toBytes(2L));
        p.setAttribute(HBaseTable.DELTA_WRITE, TRUE);
        region.put(p);
        counter2 += 2;
      }

      for (int i = 0; i < 12; i++) {
        for (int k = 0; k < 4; k++) {
          long[] uvbs = new long[k];
          for (int l = 0; l < uvbs.length; l++) {
            uvbs[l] = start + (k + 1) * (l + 1);
          }
          verifyCounts(region, new Scan().setMaxVersions(), new long[]{counter1, counter2}, i > 0 ? i : -1, uvbs);
        }
      }

    } finally {
      region.close();
    }
  }

  private Put generateIncrementPut(byte[] familyBytes, byte[] columnBytes, byte [] row, long ts) {
    Put p = new Put(row);
    p.add(familyBytes, columnBytes, ts, Bytes.toBytes(1L));
    p.setAttribute(HBaseTable.DELTA_WRITE, TRUE);
    return p;
  }

  private void verifyCounts(HRegion region, Scan scan, long[] counts) throws Exception {
    verifyCounts(region, scan, counts, -1);
  }

  private void verifyCounts(HRegion region, Scan scan, long[] counts, int batch) throws Exception {
    RegionScanner scanner = new IncrementSummingScanner(region, batch, region.getScanner(scan), ScanType.USER_SCAN);
    // init with false if loop will execute zero times
    boolean hasMore = counts.length > 0;
    for (long count : counts) {
      List<Cell> results = Lists.newArrayList();
      hasMore = scanner.next(results);
      assertEquals(1, results.size());
      Cell cell = results.get(0);
      assertNotNull(cell);
      assertEquals(count, Bytes.toLong(cell.getValue()));
    }
    assertFalse(hasMore);
  }

  private void verifyCounts(HRegion region, Scan scan, long[] counts, int batch, long[] upperVisBound)
      throws Exception {
    // The idea is to chain IncrementSummingScanner: first couple respect the upperVisBound and may produce multiple
    // cells for single value. This is what happens during flush or compaction. Second one will mimic user scan over
    // flushed or compacted: it should merge all delta increments appropriately.
    RegionScanner scanner = region.getScanner(scan);

    for (int i = 0; i < upperVisBound.length; i++) {
      scanner = new IncrementSummingScanner(region, batch, scanner,
          ScanType.COMPACT_RETAIN_DELETES, upperVisBound[i], -1);
    }
    scanner = new IncrementSummingScanner(region, batch, scanner, ScanType.USER_SCAN);
    // init with false if loop will execute zero times
    boolean hasMore = counts.length > 0;
    for (long count : counts) {
      List<Cell> results = Lists.newArrayList();
      hasMore = scanner.next(results);
      assertEquals(1, results.size());
      Cell cell = results.get(0);
      assertNotNull(cell);
      assertEquals(count, Bytes.toLong(cell.getValue()));
    }
    assertFalse(hasMore);
  }

  private HRegion createRegion(TableId tableId, byte[] family) throws Exception {
    return createRegion(conf, cConf, tableId, new HColumnDescriptor(family));
  }

  static HRegion createRegion(Configuration hConf, CConfiguration cConf, TableId tableId,
                              HColumnDescriptor cfd) throws Exception {
    HBaseTableUtil tableUtil = new HBaseTableUtilFactory(cConf).get();
    HTableDescriptorBuilder htd = tableUtil.buildHTableDescriptor(tableId);
    cfd.setMaxVersions(Integer.MAX_VALUE);
    cfd.setKeepDeletedCells(true);
    htd.addFamily(cfd);
    htd.addCoprocessor(IncrementHandler.class.getName());

    HTableDescriptor desc = htd.build();
    String tableName = desc.getNameAsString();
    Path tablePath = new Path("/tmp/" + tableName);
    Path hlogPath = new Path("/tmp/hlog-" + tableName);
    FileSystem fs = FileSystem.get(hConf);
    assertTrue(fs.mkdirs(tablePath));
    WALFactory walFactory = new WALFactory(hConf, null, hlogPath.toString());
    WAL hLog = walFactory.getWAL(new byte[]{1});
    HRegionInfo regionInfo = new HRegionInfo(desc.getTableName());
    HRegionFileSystem regionFS = HRegionFileSystem.createRegionOnFileSystem(hConf, fs, tablePath, regionInfo);
    return new HRegion(regionFS, hLog, hConf, desc,
                       new LocalRegionServerServices(hConf, ServerName.valueOf(
                           InetAddress.getLocalHost().getHostName(), 0, System.currentTimeMillis())));
  }

  private static class LocalRegionServerServices extends MockRegionServerServices {
    private final ServerName serverName;

    public LocalRegionServerServices(Configuration conf, ServerName serverName) {
      super(conf);
      this.serverName = serverName;
    }

    @Override
    public ServerName getServerName() {
      return serverName;
    }
  }
}
