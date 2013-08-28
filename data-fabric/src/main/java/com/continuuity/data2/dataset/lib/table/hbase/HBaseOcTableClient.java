package com.continuuity.data2.dataset.lib.table.hbase;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.table.Scanner;
import com.continuuity.data2.dataset.lib.table.BackedByVersionedStoreOcTableClient;
import com.continuuity.data2.transaction.Transaction;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Dataset client for HBase tables.
 */
// todo: do periodic flush when certain threshold is reached
// todo: extract separate "no delete inside tx" table?
// todo: consider writing & reading using HTable to do in multi-threaded way
public class HBaseOcTableClient extends BackedByVersionedStoreOcTableClient {
  private static final byte[] DATA_COLFAM = HBaseOcTableManager.DATA_COLUMN_FAMILY;
  // 4Mb
  private static final int DEFAULT_WRITE_BUFFER_SIZE = 4 * 1024 * 1024;

  private final HTable hTable;
  private final String hTableName;

  private Transaction tx;

  public HBaseOcTableClient(String name, CConfiguration cConf, Configuration hConf)
    throws IOException {
    super(name);
    hTableName = HBaseTableUtil.getHBaseTableName(cConf, name);
    HTable hTable = new HTable(hConf, hTableName);
    // todo: make configurable
    hTable.setWriteBufferSize(DEFAULT_WRITE_BUFFER_SIZE);
    hTable.setAutoFlush(false);
    this.hTable = hTable;
  }

  // for testing only
  String getHBaseTableName() {
    return this.hTableName;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
                  .add("hTable", hTable)
                  .add("hTableName", hTableName)
                  .toString();
  }

  @Override
  public void startTx(Transaction tx) {
    super.startTx(tx);
    this.tx = tx;
  }

  @Override
  protected void persist(NavigableMap<byte[], NavigableMap<byte[], byte[]>> buff) throws Exception {
    List<Put> puts = Lists.newArrayList();
    for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> row : buff.entrySet()) {
      Put put = new Put(row.getKey());
      for (Map.Entry<byte[], byte[]> column : row.getValue().entrySet()) {
        // we want support tx and non-tx modes
        if (tx != null) {
          // TODO: hijacking timestamp... bad
          put.add(DATA_COLFAM, column.getKey(), tx.getWritePointer(), wrapDeleteIfNeeded(column.getValue()));
        } else {
          put.add(DATA_COLFAM, column.getKey(), column.getValue());
        }
      }
      puts.add(put);
    }
    hTable.put(puts);
    hTable.flushCommits();
  }

  @Override
  protected void undo(NavigableMap<byte[], NavigableMap<byte[], byte[]>> persisted) throws Exception {
    // NOTE: we use Put with null values because we need to delete only specific version, while Delete op deletes all
    //       up to specified
    List<Put> puts = Lists.newArrayList();
    for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> row : persisted.entrySet()) {
      Put put = new Put(row.getKey());
      for (Map.Entry<byte[], byte[]> column : row.getValue().entrySet()) {
        // we want support tx and non-tx modes
        if (tx != null) {
          // TODO: hijacking timestamp... bad
          put.add(DATA_COLFAM, column.getKey(), tx.getWritePointer(), null);
        } else {
          put.add(DATA_COLFAM, column.getKey(), null);
        }
      }
      puts.add(put);
    }
    hTable.put(puts);
    hTable.flushCommits();
  }

  @Override
  protected byte[] getPersisted(byte[] row, byte[] column) throws Exception {
    NavigableMap<byte[], byte[]> result = getInternal(row, new byte[][]{column});
    return result.get(column);
  }

  @Override
  protected NavigableMap<byte[], byte[]> getPersisted(byte[] row, byte[] startColumn, byte[] stopColumn, int limit)
    throws Exception {

    // todo: this is very inefficient: column range + limit should be pushed down via server-side filters (see
    //       HBaseOVCTable implementation)
    return getRange(getInternal(row, null), startColumn, stopColumn, limit);
  }

  @Override
  protected NavigableMap<byte[], byte[]> getPersisted(byte[] row, byte[][] columns) throws Exception {
    return getInternal(row, columns);
  }

  @Override
  protected Scanner scanPersisted(byte[] startRow, byte[] stopRow) throws Exception {
    Scan scan = new Scan();
    scan.addFamily(DATA_COLFAM);
    // todo: should be configurable
    // NOTE: by default we assume scanner is used in mapreduce job, hence no cache blocks
    scan.setCacheBlocks(false);
    scan.setCaching(1000);

    if (startRow != null) {
      scan.setStartRow(startRow);
    }
    if (stopRow != null) {
      scan.setStopRow(stopRow);
    }

    scan.setTimeRange(0, getMaxStamp(tx));
    // todo: optimise for no excluded list separately
    scan.setMaxVersions(tx.getExcludedList().length + 1);

    ResultScanner resultScanner = hTable.getScanner(scan);
    return new HBaseScanner(resultScanner, tx.getExcludedList());
  }

  private NavigableMap<byte[], byte[]> getInternal(byte[] row, byte[][] columns) throws IOException {
    Get get = new Get(row);
    // todo: uncomment when doing caching fetching data in-memory
    // get.setCacheBlocks(false);
    get.addFamily(DATA_COLFAM);
    if (columns != null) {
      for (byte[] column : columns) {
        get.addColumn(DATA_COLFAM, column);
      }
    }

    // no tx logic needed
    if (tx == null) {
      get.setMaxVersions(1);
      Result result = hTable.get(get);
      return result.isEmpty() ? EMPTY_ROW_MAP : result.getFamilyMap(DATA_COLFAM);
    }

    // todo: actually we want to read up to write pointer... when we start flushing periodically
    get.setTimeRange(0L, getMaxStamp(tx));

    // if exclusion list is empty, do simple "read last" value call todo: explain
    if (tx.getExcludedList().length == 0) {
      get.setMaxVersions(1);
      Result result = hTable.get(get);
      if (result.isEmpty()) {
        return EMPTY_ROW_MAP;
      }
      NavigableMap<byte[], byte[]> rowMap = result.getFamilyMap(DATA_COLFAM);
      return unwrapDeletes(rowMap);
    }

//   todo: provide max known not excluded version, so that we can figure out how to fetch even fewer versions
//         on the other hand, looks like the above suggestion WILL NOT WORK
    get.setMaxVersions(tx.getExcludedList().length + 1);

    // todo: push filtering logic to server
    // todo: cache fetched from server locally

    Result result = hTable.get(get);
    return getRowMap(result, tx.getExcludedList());
  }

  static NavigableMap<byte[], byte[]> getRowMap(Result result, long[] excludedVersions) {
    if (result.isEmpty()) {
      return EMPTY_ROW_MAP;
    }

    NavigableMap<byte[], byte[]> rowMap = getLatestNotExcluded(result.getMap().get(DATA_COLFAM), excludedVersions);
    return unwrapDeletes(rowMap);
  }

  private static long getMaxStamp(Transaction tx) {
    // NOTE: +1 here because we want read up to readpointer inclusive, but timerange's end is exclusive
    return tx.getReadPointer() + 1;
  }
}
