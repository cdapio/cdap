/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.data2.dataset2.lib.table.hbase;

import com.continuuity.api.dataset.table.ConflictDetection;
import com.continuuity.api.dataset.table.Scanner;
import com.continuuity.data2.dataset2.lib.table.BackedByVersionedStoreOrderedTable;
import com.continuuity.data2.util.hbase.HBaseTableUtil;
import com.continuuity.tephra.Transaction;
import com.continuuity.tephra.TransactionCodec;
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
public class HBaseOrderedTable extends BackedByVersionedStoreOrderedTable {
  private final HTable hTable;
  private final int ttl;

  private Transaction tx;

  private final TransactionCodec txCodec;

  protected HBaseOrderedTable(String name, Configuration hConf, ConflictDetection level, int ttl) throws IOException {
    super(name, level);
    HTable hTable = new HTable(hConf, getTransactionAwareName());
    this.ttl = ttl;
    // todo: make configurable
    hTable.setWriteBufferSize(HBaseTableUtil.DEFAULT_WRITE_BUFFER_SIZE);
    hTable.setAutoFlush(false);
    this.hTable = hTable;
    this.txCodec = new TransactionCodec();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
                  .add("hTable", hTable)
                  .add("hTableName", getTransactionAwareName())
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
          put.add(HBaseOrderedTableAdmin.DATA_COLUMN_FAMILY, column.getKey(), tx.getWritePointer(),
                  wrapDeleteIfNeeded(column.getValue()));
        } else {
          put.add(HBaseOrderedTableAdmin.DATA_COLUMN_FAMILY, column.getKey(), column.getValue());
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
          delete.deleteColumn(HBaseOrderedTableAdmin.DATA_COLUMN_FAMILY, column.getKey(), tx.getWritePointer());
        } else {
          delete.deleteColumn(HBaseOrderedTableAdmin.DATA_COLUMN_FAMILY, column.getKey());
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
    scan.addFamily(HBaseOrderedTableAdmin.DATA_COLUMN_FAMILY);
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

    txCodec.addToOperation(scan, tx);

    ResultScanner resultScanner = hTable.getScanner(scan);
    return new HBaseScanner(resultScanner, tx);
  }

  private NavigableMap<byte[], byte[]> getInternal(byte[] row, @Nullable byte[][] columns) throws IOException {
    Get get = new Get(row);
    // todo: uncomment when doing caching fetching data in-memory
    // get.setCacheBlocks(false);
    get.addFamily(HBaseOrderedTableAdmin.DATA_COLUMN_FAMILY);
    if (columns != null) {
      for (byte[] column : columns) {
        get.addColumn(HBaseOrderedTableAdmin.DATA_COLUMN_FAMILY, column);
      }
    }

    // no tx logic needed
    if (tx == null) {
      get.setMaxVersions(1);
      Result result = hTable.get(get);
      return result.isEmpty() ? EMPTY_ROW_MAP : result.getFamilyMap(HBaseOrderedTableAdmin.DATA_COLUMN_FAMILY);
    }

    txCodec.addToOperation(get, tx);

    Result result = hTable.get(get);
    return getRowMap(result, tx);
  }

  static NavigableMap<byte[], byte[]> getRowMap(Result result, Transaction tx) {
    if (result.isEmpty()) {
      return EMPTY_ROW_MAP;
    }

    NavigableMap<byte[], byte[]> rowMap =
      getLatestNotExcluded(result.getMap().get(HBaseOrderedTableAdmin.DATA_COLUMN_FAMILY), tx);
    return unwrapDeletes(rowMap);
  }
}
