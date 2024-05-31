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

package io.cdap.cdap.data2.increment.hbase10;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import io.cdap.cdap.data2.increment.hbase.AbstractIncrementHandlerTest;
import io.cdap.cdap.data2.increment.hbase.IncrementHandlerState;
import io.cdap.cdap.data2.increment.hbase.TimestampOracle;
import io.cdap.cdap.data2.util.TableId;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtil;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtilFactory;
import io.cdap.cdap.data2.util.hbase.HTableDescriptorBuilder;
import io.cdap.cdap.test.SlowTests;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.experimental.categories.Category;

/**
 * Tests for the HBase 1.0 version of the {@link IncrementHandler} coprocessor.
 */
@Category(SlowTests.class)
public class IncrementHandlerTest extends AbstractIncrementHandlerTest {

  @Override
  public void assertColumn(Table table, byte[] row, byte[] col, long expected) throws Exception {
    Result res = table.get(new Get(row));
    Cell resA = res.getColumnLatestCell(FAMILY, col);
    assertFalse(res.isEmpty());
    assertNotNull(resA);
    assertEquals(expected, Bytes.toLong(resA.getValue()));

    Scan scan = new Scan(row);
    scan.addFamily(FAMILY);
    ResultScanner scanner = table.getScanner(scan);
    Result scanRes = scanner.next();
    assertNotNull(scanRes);
    assertFalse(scanRes.isEmpty());
    Cell scanResA = scanRes.getColumnLatestCell(FAMILY, col);
    assertArrayEquals(row, scanResA.getRow());
    assertEquals(expected, Bytes.toLong(scanResA.getValue()));
  }

  @Override
  public void assertColumns(Table table, byte[] row, byte[][] cols, long[] expected) throws Exception {
    assertEquals(cols.length, expected.length);

    Get get = new Get(row);
    Scan scan = new Scan(row);
    for (byte[] col : cols) {
      get.addColumn(FAMILY, col);
      scan.addColumn(FAMILY, col);
    }

    // check get
    Result res = table.get(get);
    assertFalse(res.isEmpty());
    for (int i = 0; i < cols.length; i++) {
      Cell resCell = res.getColumnLatestCell(FAMILY, cols[i]);
      assertNotNull(resCell);
      assertEquals(expected[i], Bytes.toLong(resCell.getValue()));
    }

    // check scan
    ResultScanner scanner = table.getScanner(scan);
    Result scanRes = scanner.next();
    assertNotNull(scanRes);
    assertFalse(scanRes.isEmpty());
    for (int i = 0; i < cols.length; i++) {
      Cell scanResCell = scanRes.getColumnLatestCell(FAMILY, cols[i]);
      assertArrayEquals(row, scanResCell.getRow());
      assertEquals(expected[i], Bytes.toLong(scanResCell.getValue()));
    }
  }

  @Override
  public Table createTable(TableId tableId) throws Exception {
    HBaseTableUtil tableUtil = new HBaseTableUtilFactory(cConf).get();
    HTableDescriptorBuilder tableDesc = tableUtil.buildHTableDescriptor(tableId);
    HColumnDescriptor columnDesc = new HColumnDescriptor(FAMILY);
    columnDesc.setMaxVersions(Integer.MAX_VALUE);
    columnDesc.setValue(IncrementHandlerState.PROPERTY_TRANSACTIONAL, "false");
    tableDesc.addFamily(columnDesc);
    tableDesc.addCoprocessor(IncrementHandler.class.getName());
    HTableDescriptor htd = tableDesc.build();
    TEST_HBASE.getHBaseAdmin().createTable(htd);
    TEST_HBASE.waitUntilTableAvailable(htd.getName(), 5000);
    return tableUtil.createTable(conf, tableId);
  }

  @Override
  public RegionWrapper createRegion(TableId tableId, Map<String, String> familyProperties) throws Exception {
    HColumnDescriptor columnDesc = new HColumnDescriptor(FAMILY);
    columnDesc.setMaxVersions(Integer.MAX_VALUE);
    for (Map.Entry<String, String> prop : familyProperties.entrySet()) {
      columnDesc.setValue(prop.getKey(), prop.getValue());
    }
    return new HBase98RegionWrapper(
        IncrementSummingScannerTest.createRegion(TEST_HBASE.getConfiguration(), cConf, tableId, columnDesc));
  }

  public static ColumnCell convertCell(Cell cell) {
    return new ColumnCell(CellUtil.cloneRow(cell), CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell),
        cell.getTimestamp(), CellUtil.cloneValue(cell));
  }

  public class HBase98RegionWrapper implements RegionWrapper {
    private final HRegion region;

    public HBase98RegionWrapper(HRegion region) {
      this.region = region;
    }

    @Override
    public void initialize() throws IOException {
      region.initialize();
    }

    @Override
    public void put(Put put) throws IOException {
      region.put(put);
    }

    @Override
    public boolean scanRegion(List<ColumnCell> results, byte[] startRow) throws IOException {
      return scanRegion(results, startRow, null);
    }

    @Override
    public boolean scanRegion(List<ColumnCell> results, byte[] startRow, byte[][] columns) throws IOException {
      Scan scan = new Scan().setMaxVersions().setStartRow(startRow);
      if (columns != null) {
        for (int i = 0; i < columns.length; i++) {
          scan.addColumn(FAMILY, columns[i]);
        }
      }
      RegionScanner rs = region.getScanner(scan);
      try {
        List<Cell> tmpResults = new ArrayList<>();
        boolean hasMore = rs.next(tmpResults);
        for (Cell cell : tmpResults) {
          results.add(convertCell(cell));
        }
        return hasMore;
      } finally {
        rs.close();
      }
    }

    @Override
    public boolean flush() throws IOException {
      HRegion.FlushResult result = region.flushcache();
      return result.isCompactionNeeded();
    }

    @Override
    public void compact(boolean majorCompact) throws IOException {
      region.compactStores(majorCompact);
    }

    @Override
    public void setCoprocessorTimestampOracle(TimestampOracle timeOracle) {
      Coprocessor cp = region.getCoprocessorHost().findCoprocessor(IncrementHandler.class.getName());
      assertNotNull(cp);
      ((IncrementHandler) cp).setTimestampOracle(timeOracle);
    }

    @Override
    public void close() throws IOException {
      region.close();
    }
  }
}
