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
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data2.dataset2.lib.table.FuzzyRowFilter;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.data2.util.hbase.TableId;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import javax.annotation.Nullable;

/**
 * An HBase metrics table client.
 */
public class HBaseMetricsTable implements MetricsTable {
  private final HTable hTable;
  private final byte[] columnFamily;

  public HBaseMetricsTable(DatasetSpecification spec, Configuration hConf) throws IOException {
    String hTableName = HBaseTableUtil.getHBaseTableName(spec.getName());
    HBaseTableUtil tableUtil = new HBaseTableUtilFactory().get();
    HTable hTable = tableUtil.getHTable(hConf, TableId.from(hTableName));
    // todo: make configurable
    hTable.setWriteBufferSize(HBaseTableUtil.DEFAULT_WRITE_BUFFER_SIZE);
    hTable.setAutoFlush(false);
    this.hTable = hTable;
    this.columnFamily = HBaseTableAdmin.getColumnFamily(spec);
  }

  @Override
  @Nullable
  public byte[] get(byte[] row, byte[] column) throws Exception {
    Get get = new Get(row);
    get.addColumn(columnFamily, column);
    get.setMaxVersions(1);
    Result getResult = hTable.get(get);
    if (!getResult.isEmpty()) {
      return getResult.getValue(columnFamily, column);
    }
    return null;
  }

  @Override
  public void put(NavigableMap<byte[], NavigableMap<byte[], Long>> updates) throws Exception {
    List<Put> puts = Lists.newArrayList();
    for (Map.Entry<byte[], NavigableMap<byte[], Long>> row : updates.entrySet()) {
      Put put = new Put(row.getKey());
      for (Map.Entry<byte[], Long> column : row.getValue().entrySet()) {
        put.add(columnFamily, column.getKey(), Bytes.toBytes(column.getValue()));
      }
      puts.add(put);
    }
    hTable.put(puts);
    hTable.flushCommits();
  }

  @Override
  public boolean swap(byte[] row, byte[] column, byte[] oldValue, byte[] newValue) throws Exception {
    if (newValue == null) {
      Delete delete = new Delete(row);
      // HBase API weirdness: we must use deleteColumns() because deleteColumn() deletes only the last version.
      delete.deleteColumns(columnFamily, column);
      return hTable.checkAndDelete(row, columnFamily, column, oldValue, delete);
    } else {
      Put put = new Put(row);
      put.add(columnFamily, column, newValue);
      return hTable.checkAndPut(row, columnFamily, column, oldValue, put);
    }
  }

  @Override
  public void increment(byte[] row, Map<byte[], Long> increments) throws Exception {
    Put increment = getIncrementalPut(row, increments);
    try {
      hTable.put(increment);
    } catch (IOException e) {
      // figure out whether this is an illegal increment
      // currently there is not other way to extract that from the HBase exception than string match
      if (e.getMessage() != null && e.getMessage().contains("isn't 64 bits wide")) {
        throw new NumberFormatException("Attempted to increment a value that is not convertible to long," +
                                          " row: " + Bytes.toStringBinary(row));
      }
      throw e;
    }
    hTable.flushCommits();
  }

  private Put getIncrementalPut(byte[] row, Map<byte[], Long> increments) {
    Put increment = getIncrementalPut(row);
    for (Map.Entry<byte[], Long> column : increments.entrySet()) {
      // note: we use default timestamp (current), which is fine because we know we collect metrics no more
      //       frequent than each second. We also rely on same metric value to be processed by same metric processor
      //       instance, so no conflicts are possible.
      increment.add(columnFamily, column.getKey(), Bytes.toBytes(column.getValue()));
    }
    return increment;
  }

  private Put getIncrementalPut(byte[] row) {
    Put put = new Put(row);
    put.setAttribute(HBaseTable.DELTA_WRITE, Bytes.toBytes(true));
    return put;
  }

  @Override
  public void increment(NavigableMap<byte[], NavigableMap<byte[], Long>> updates) throws Exception {
    List<Put> puts = Lists.newArrayList();
    for (Map.Entry<byte[], NavigableMap<byte[], Long>> update : updates.entrySet()) {
      Put increment = getIncrementalPut(update.getKey(), update.getValue());
      puts.add(increment);
    }

    try {
      hTable.put(puts);
    } catch (IOException e) {
      // figure out whether this is an illegal increment
      // currently there is not other way to extract that from the HBase exception than string match
      if (e.getMessage() != null && e.getMessage().contains("isn't 64 bits wide")) {
        throw new NumberFormatException("Attempted to increment a value that is not convertible to long.");
      }
      throw e;
    }
    hTable.flushCommits();
  }

  @Override
  public long incrementAndGet(byte[] row, byte[] column, long delta) throws Exception {
    Increment increment = new Increment(row);
    increment.addColumn(columnFamily, column, delta);
    try {
      Result result = hTable.increment(increment);
      return Bytes.toLong(result.getValue(columnFamily, column));
    } catch (IOException e) {
      // figure out whether this is an illegal increment
      // currently there is not other way to extract that from the HBase exception than string match
      if (e.getMessage() != null && e.getMessage().contains("isn't 64 bits wide")) {
        throw new NumberFormatException("Attempted to increment a value that is not convertible to long," +
                                          " row: " + Bytes.toStringBinary(row) +
                                          " column: " + Bytes.toStringBinary(column));
      }
      throw e;
    }
  }

  @Override
  public void deleteAll(byte[] prefix) throws Exception {
    final int deletesPerRound = 1024; // todo make configurable
    List<Delete> deletes = Lists.newArrayListWithCapacity(deletesPerRound);
    // repeatedly scan a batch rows to detect their row keys, then delete all in a single call.
    Scan scan = new Scan();
    scan.setTimeRange(0, HConstants.LATEST_TIMESTAMP);
    scan.setMaxVersions(1); // we only need to see one version of each row
    scan.setFilter(new FirstKeyOnlyFilter()); // we only need to see the first column (=key) of each row
    scan.setStartRow(prefix);
    ResultScanner scanner = this.hTable.getScanner(scan);
    try {
      Result result;
      while ((result = scanner.next()) != null) {
        byte[] rowKey = result.getRow();
        if (!Bytes.startsWith(rowKey, prefix)) {
          break;
        }
        deletes.add(new Delete(rowKey));
        // every 1024 iterations we perform the outstanding deletes
        if (deletes.size() >= deletesPerRound) {
          hTable.delete(deletes);
          deletes.clear();
        }
      }
      // perform any outstanding deletes
      if (deletes.size() > 0) {
        hTable.delete(deletes);
      }
      hTable.flushCommits();
    } finally {
      scanner.close();
    }
  }

  @Override
  public void delete(byte[] row, byte[][] columns) throws Exception {
    Delete delete = new Delete(row);
    for (byte[] column : columns) {
      delete.deleteColumns(columnFamily, column);
    }
    hTable.delete(delete);
  }

  @Override
  public void delete(Collection<byte[]> rows) throws Exception {
    List<Delete> deletes = Lists.newArrayList();
    for (byte[] row : rows) {
      deletes.add(new Delete(row));
    }
    hTable.delete(deletes);
  }


  @Override
  public void deleteRange(@Nullable byte[] startRow, @Nullable byte[] stopRow,
                          @Nullable byte[][] columns, @Nullable FuzzyRowFilter filter) throws IOException {
    final int deletesPerRound = 1024; // todo make configurable
    List<Delete> deletes = Lists.newArrayListWithCapacity(deletesPerRound);
    // repeatedly scan a batch rows to detect their row keys, then delete all in a single call.
    Scan scan = new Scan();
    scan.setTimeRange(0, HConstants.LATEST_TIMESTAMP);
    configureRangeScan(scan, startRow, stopRow, columns, filter);
    ResultScanner scanner = this.hTable.getScanner(scan);
    try {
      Result result;
      while ((result = scanner.next()) != null) {
        byte[] rowKey = result.getRow();
        Delete delete = new Delete(rowKey);
        if (columns != null) {
          for (byte[] column : columns) {
            delete.deleteColumns(columnFamily, column);
          }
        }
        deletes.add(delete);
        // every 1024 iterations we perform the outstanding deletes
        if (deletes.size() >= deletesPerRound) {
          hTable.delete(deletes);
          deletes.clear();
        }
      }
      // perform any outstanding deletes
      if (deletes.size() > 0) {
        hTable.delete(deletes);
      }
      hTable.flushCommits();
    } finally {
      scanner.close();
    }
  }

  @Override
  public Scanner scan(@Nullable byte[] startRow, @Nullable byte[] stopRow,
                      @Nullable byte[][] columns, @Nullable FuzzyRowFilter filter) throws IOException {
    Scan scan = new Scan();
    configureRangeScan(scan, startRow, stopRow, columns, filter);
    ResultScanner resultScanner = hTable.getScanner(scan);
    return new HBaseScanner(resultScanner, columnFamily);
  }

  private Scan configureRangeScan(Scan scan, @Nullable byte[] startRow, @Nullable byte[] stopRow,
                                  @Nullable byte[][] columns, @Nullable FuzzyRowFilter filter) {
    // todo: should be configurable
    scan.setCaching(1000);

    if (startRow != null) {
      scan.setStartRow(startRow);
    }
    if (stopRow != null) {
      scan.setStopRow(stopRow);
    }
    if (columns != null) {
      for (byte[] column : columns) {
        scan.addColumn(columnFamily, column);
      }
    } else {
      scan.addFamily(columnFamily);
    }
    if (filter != null) {
      List<Pair<byte[], byte[]>> fuzzyPairs = Lists.newArrayListWithExpectedSize(filter.getFuzzyKeysData().size());
      for (ImmutablePair<byte[], byte[]> pair : filter.getFuzzyKeysData()) {
        fuzzyPairs.add(Pair.newPair(pair.getFirst(), pair.getSecond()));
      }
      scan.setFilter(new org.apache.hadoop.hbase.filter.FuzzyRowFilter(fuzzyPairs));
    }
    return scan;
  }

  @Override
  public void close() throws IOException {
    hTable.close();
  }
}
