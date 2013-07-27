package com.continuuity.data2.dataset.lib.table.hbase;

import com.continuuity.data.table.Scanner;
import com.continuuity.data2.dataset.lib.table.BackedByVersionedStoreOcTableClient;
import com.continuuity.data2.transaction.Transaction;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 *
 */
// todo: do periodic flush when certain threshold is reached
// todo: extract separate "no delete inside tx" table?
public class HBaseOcTableClient extends BackedByVersionedStoreOcTableClient {
  private static final byte[] DATA_COLFAM = HBaseOcTableManager.DATA_COLUMN_FAMILY;

  private final HTable hTable;

  private Transaction tx;

  public HBaseOcTableClient(String name, Configuration hConf)
    throws IOException {
    super(name);
    this.hTable = new HTable(hConf, name);
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
    }
    hTable.put(puts);
  }

  @Override
  protected NavigableMap<byte[], byte[]> getPersisted(byte[] row) throws Exception {
    return getInternal(row, null);
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
  protected Scanner scanPersisted(byte[] startRow, byte[] stopRow) {
    // todo
    return null;
  }

  private NavigableMap<byte[], byte[]> getInternal(byte[] row, byte[][] columns) throws IOException {
    Get get = new Get(row);
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
    get.setTimeRange(0L, tx.getReadPointer());

    // if exclusion list is empty, do simple "read last" value call todo: explain
    if (tx.getExcludedList().length == 0) {
      get.setMaxVersions(1);
      NavigableMap<byte[], byte[]> rowMap = hTable.get(get).getFamilyMap(DATA_COLFAM);
      return unwrapDeletes(rowMap);
    }

//   todo: provide max known not excluded version, so that we can figure out how to fetch even fewer versions
//         on the other hand, looks like the above suggestion WILL NOT WORK
    get.setMaxVersions(tx.getExcludedList().length + 1);

    // todo: push filtering logic to server
    // todo: cache fetched from server locally

    Result result = hTable.get(get);
    if (result.isEmpty()) {
      return EMPTY_ROW_MAP;
    }

    NavigableMap<byte[], byte[]> rowMap = getLatestNotExcluded(result.getMap().get(DATA_COLFAM),
                                                               tx.getExcludedList());
    return unwrapDeletes(rowMap);
  }
}
