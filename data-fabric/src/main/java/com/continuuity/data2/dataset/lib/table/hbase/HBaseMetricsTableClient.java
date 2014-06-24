/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.data2.dataset.lib.table.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.table.Scanner;
import com.continuuity.data2.OperationException;
import com.continuuity.data2.OperationResult;
import com.continuuity.data2.dataset.lib.table.FuzzyRowFilter;
import com.continuuity.data2.dataset.lib.table.MetricsTable;
import com.continuuity.data2.util.hbase.HBaseTableUtil;
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
import javax.annotation.Nullable;

/**
 * An HBase metrics table client.
 */
public class HBaseMetricsTableClient implements MetricsTable {

  private final HTable hTable;

  public HBaseMetricsTableClient(String name, Configuration hConf)
    throws IOException {
    String hTableName = HBaseTableUtil.getHBaseTableName(name);
    HTable hTable = new HTable(hConf, hTableName);
    // todo: make configurable
    hTable.setWriteBufferSize(HBaseTableUtil.DEFAULT_WRITE_BUFFER_SIZE);
    hTable.setAutoFlush(false);
    this.hTable = hTable;
  }

  @Override
  public OperationResult<byte[]> get(byte[] row, byte[] column) throws Exception {
    Get get = new Get(row);
    get.addColumn(HBaseOcTableManager.DATA_COLUMN_FAMILY, column);
    get.setMaxVersions(1);
    Result getResult = hTable.get(get);
    if (!getResult.isEmpty()) {
      byte[] value = getResult.getValue(HBaseOcTableManager.DATA_COLUMN_FAMILY, column);
      if (value != null) {
        return new OperationResult<byte[]>(value);
      }
    }
    return new OperationResult<byte[]>(StatusCode.KEY_NOT_FOUND);
  }

  @Override
  public void put(Map<byte[], Map<byte[], byte[]>> updates) throws Exception {
    List<Put> puts = Lists.newArrayList();
    for (Map.Entry<byte[], Map<byte[], byte[]>> row : updates.entrySet()) {
      Put put = new Put(row.getKey());
      for (Map.Entry<byte[], byte[]> column : row.getValue().entrySet()) {
        put.add(HBaseOcTableManager.DATA_COLUMN_FAMILY, column.getKey(), column.getValue());
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
      delete.deleteColumns(HBaseOcTableManager.DATA_COLUMN_FAMILY, column);
      return hTable.checkAndDelete(row, HBaseOcTableManager.DATA_COLUMN_FAMILY, column, oldValue, delete);
    } else {
      Put put = new Put(row);
      put.add(HBaseOcTableManager.DATA_COLUMN_FAMILY, column, newValue);
      return hTable.checkAndPut(row, HBaseOcTableManager.DATA_COLUMN_FAMILY, column, oldValue, put);
    }
  }

  @Override
  public void increment(byte[] row, Map<byte[], Long> increments) throws Exception {
    Increment increment = new Increment(row);
    for (Map.Entry<byte[], Long> column : increments.entrySet()) {
      increment.addColumn(HBaseOcTableManager.DATA_COLUMN_FAMILY, column.getKey(), column.getValue());
    }
    try {
      hTable.increment(increment);
    } catch (IOException e) {
      // figure out whether this is an illegal increment
      // currently there is not other way to extract that from the HBase exception than string match
      if (e.getMessage() != null && e.getMessage().contains("isn't 64 bits wide")) {
        throw new OperationException(StatusCode.ILLEGAL_INCREMENT, e.getMessage(), e);
      }
      throw e;
    }
    hTable.flushCommits();
  }

  @Override
  public long incrementAndGet(byte[] row, byte[] column, long delta) throws Exception {
    Increment increment = new Increment(row);
    increment.addColumn(HBaseOcTableManager.DATA_COLUMN_FAMILY, column, delta);
    try {
      Result result = hTable.increment(increment);
      return Bytes.toLong(result.getValue(HBaseOcTableManager.DATA_COLUMN_FAMILY, column));
    } catch (IOException e) {
      // figure out whether this is an illegal increment
      // currently there is not other way to extract that from the HBase exception than string match
      if (e.getMessage() != null && e.getMessage().contains("isn't 64 bits wide")) {
        throw new OperationException(StatusCode.ILLEGAL_INCREMENT, e.getMessage(), e);
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
            delete.deleteColumns(HBaseOcTableManager.DATA_COLUMN_FAMILY, column);
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
    return new HBaseScanner(resultScanner, null);
  }

  private Scan configureRangeScan(Scan scan, @Nullable byte[] startRow, @Nullable byte[] stopRow,
                                  @Nullable byte[][] columns, @Nullable FuzzyRowFilter filter) {
    // todo: should be configurable
    scan.setCaching(1000);
    scan.setMaxVersions(1);

    if (startRow != null) {
      scan.setStartRow(startRow);
    }
    if (stopRow != null) {
      scan.setStopRow(stopRow);
    }
    if (columns != null) {
      for (byte[] column : columns) {
        scan.addColumn(HBaseOcTableManager.DATA_COLUMN_FAMILY, column);
      }
    } else {
      scan.addFamily(HBaseOcTableManager.DATA_COLUMN_FAMILY);
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
}
