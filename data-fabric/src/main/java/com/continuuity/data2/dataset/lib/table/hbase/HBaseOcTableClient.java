package com.continuuity.data2.dataset.lib.table.hbase;

import com.continuuity.data.table.Scanner;
import com.continuuity.data2.dataset.lib.table.BackedByVersionedStoreOcTableClient;
import com.continuuity.data2.dataset.lib.table.ConflictDetection;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.util.hbase.HBaseTableUtil;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
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
import javax.annotation.Nullable;

/**
 * Dataset client for HBase tables.
 */
// todo: do periodic flush when certain threshold is reached
// todo: extract separate "no delete inside tx" table?
// todo: consider writing & reading using HTable to do in multi-threaded way
public class HBaseOcTableClient extends BackedByVersionedStoreOcTableClient {
  private final HTable hTable;
  private final String hTableName;
  private final int ttl;

  private Transaction tx;
  /** oldest visible based on ttl */
  private long oldestVisible;

  public HBaseOcTableClient(String name, Configuration hConf) throws IOException {
    this(name, ConflictDetection.ROW, hConf);
  }

  public HBaseOcTableClient(String name, ConflictDetection level, Configuration hConf) throws IOException {
    this(name, level, -1, hConf);
  }

  public HBaseOcTableClient(String name, ConflictDetection level, int ttl, Configuration hConf) throws IOException {
    super(name, level);
    this.ttl = ttl;
    hTableName = HBaseTableUtil.getHBaseTableName(name);
    HTable hTable = new HTable(hConf, hTableName);
    // todo: make configurable
    hTable.setWriteBufferSize(HBaseTableUtil.DEFAULT_WRITE_BUFFER_SIZE);
    hTable.setAutoFlush(false);
    this.hTable = hTable;
  }

  // for testing only
  public String getHBaseTableName() {
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
    // we know that data will not be cleaned up while this tx is running up to this point as janitor uses it
    this.oldestVisible = ttl <= 0 ? 0 : tx.getVisibilityUpperBound() - ttl * TxConstants.MAX_TX_PER_MS;
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
          put.add(HBaseOcTableManager.DATA_COLUMN_FAMILY, column.getKey(), tx.getWritePointer(),
                  wrapDeleteIfNeeded(column.getValue()));
        } else {
          put.add(HBaseOcTableManager.DATA_COLUMN_FAMILY, column.getKey(), column.getValue());
        }
      }
      puts.add(put);
    }
    hTable.put(puts);
    hTable.flushCommits();
  }

  @Override
  protected void undo(NavigableMap<byte[], NavigableMap<byte[], byte[]>> persisted) throws Exception {
    // NOTE: we use Delete with the write pointer as the specific version to delete.
    List<Delete> deletes = Lists.newArrayList();
    for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> row : persisted.entrySet()) {
      Delete delete = new Delete(row.getKey());
      for (Map.Entry<byte[], byte[]> column : row.getValue().entrySet()) {
        // we want support tx and non-tx modes
        if (tx != null) {
          // TODO: hijacking timestamp... bad
          delete.deleteColumn(HBaseOcTableManager.DATA_COLUMN_FAMILY, column.getKey(), tx.getWritePointer());
        } else {
          delete.deleteColumn(HBaseOcTableManager.DATA_COLUMN_FAMILY, column.getKey());
        }
      }
      deletes.add(delete);
    }
    hTable.delete(deletes);
    hTable.flushCommits();
  }

  @Override
  protected NavigableMap<byte[], byte[]> getPersisted(byte[] row, byte[] startColumn, byte[] stopColumn, int limit)
    throws Exception {

    // todo: this is very inefficient: column range + limit should be pushed down via server-side filters
    return getRange(getInternal(row, null), startColumn, stopColumn, limit);
  }

  @Override
  protected NavigableMap<byte[], byte[]> getPersisted(byte[] row, @Nullable byte[][] columns) throws Exception {
    return getInternal(row, columns);
  }

  @Override
  protected Scanner scanPersisted(byte[] startRow, byte[] stopRow) throws Exception {
    Scan scan = new Scan();
    scan.addFamily(HBaseOcTableManager.DATA_COLUMN_FAMILY);
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

    scan.setTimeRange(oldestVisible, getMaxStamp(tx));
    // todo: optimise for no excluded list separately
    scan.setMaxVersions(tx.excludesSize() + 1);

    ResultScanner resultScanner = hTable.getScanner(scan);
    return new HBaseScanner(resultScanner, tx);
  }

  private NavigableMap<byte[], byte[]> getInternal(byte[] row, @Nullable byte[][] columns) throws IOException {
    Get get = new Get(row);
    // todo: uncomment when doing caching fetching data in-memory
    // get.setCacheBlocks(false);
    get.addFamily(HBaseOcTableManager.DATA_COLUMN_FAMILY);
    if (columns != null) {
      for (byte[] column : columns) {
        get.addColumn(HBaseOcTableManager.DATA_COLUMN_FAMILY, column);
      }
    }

    // no tx logic needed
    if (tx == null) {
      get.setMaxVersions(1);
      Result result = hTable.get(get);
      return result.isEmpty() ? EMPTY_ROW_MAP : result.getFamilyMap(HBaseOcTableManager.DATA_COLUMN_FAMILY);
    }

    // todo: actually we want to read up to write pointer... when we start flushing periodically
    get.setTimeRange(oldestVisible, getMaxStamp(tx));

    // if exclusion list is empty, do simple "read last" value call todo: explain
    if (!tx.hasExcludes()) {
      get.setMaxVersions(1);
      Result result = hTable.get(get);
      if (result.isEmpty()) {
        return EMPTY_ROW_MAP;
      }
      NavigableMap<byte[], byte[]> rowMap = result.getFamilyMap(HBaseOcTableManager.DATA_COLUMN_FAMILY);
      return unwrapDeletes(rowMap);
    }

    // todo: provide max known not excluded version, so that we can figure out how to fetch even fewer versions
    //       on the other hand, looks like the above suggestion WILL NOT WORK
    get.setMaxVersions(tx.excludesSize() + 1);

    // todo: push filtering logic to server
    // todo: cache fetched from server locally

    Result result = hTable.get(get);
    return getRowMap(result, tx);
  }

  static NavigableMap<byte[], byte[]> getRowMap(Result result, Transaction tx) {
    if (result.isEmpty()) {
      return EMPTY_ROW_MAP;
    }

    NavigableMap<byte[], byte[]> rowMap =
      getLatestNotExcluded(result.getMap().get(HBaseOcTableManager.DATA_COLUMN_FAMILY), tx);
    return unwrapDeletes(rowMap);
  }

  private static long getMaxStamp(Transaction tx) {
    // NOTE: +1 here because we want read up to readpointer inclusive, but timerange's end is exclusive
    return tx.getReadPointer() + 1;
  }
}
