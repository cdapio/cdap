/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream.hbase;

import com.continuuity.data2.transaction.queue.QueueEntryRow;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.continuuity.data2.transaction.stream.StreamConsumerStateStore;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public final class HBaseStreamConsumerStateStore extends StreamConsumerStateStore {

  private final HTable hTable;

  /**
   * Constructor to create an instance for a given stream.
   *
   * @param streamConfig configuration information of the stream.
   * @param hTable for communicating with HBase for backing store.
   */
  public HBaseStreamConsumerStateStore(StreamConfig streamConfig, HTable hTable) {
    super(streamConfig);
    this.hTable = hTable;
  }

  @Override
  public void close() throws IOException {
    hTable.close();
  }

  @Override
  protected byte[] fetch(byte[] row, byte[] column) throws IOException {
    Get get = new Get(row);
    get.addColumn(QueueEntryRow.COLUMN_FAMILY, column);
    get.setMaxVersions(1);
    Result result = hTable.get(get);
    return result.isEmpty() ? null : result.value();
  }

  @Override
  protected void fetchAll(byte[] row, Map<byte[], byte[]> result) throws IOException {
    fetchAll(row, null, result);
  }

  @Override
  protected void fetchAll(byte[] row, byte[] columnPrefix, Map<byte[], byte[]> result) throws IOException {
    Get get = new Get(row);
    get.addFamily(QueueEntryRow.COLUMN_FAMILY);
    get.setMaxVersions(1);
    if (columnPrefix != null) {
      get.setFilter(new ColumnPrefixFilter(columnPrefix));
    }
    Result hTableResult = hTable.get(get);

    if (hTableResult.isEmpty()) {
      return;
    }
    result.putAll(hTableResult.getFamilyMap(QueueEntryRow.COLUMN_FAMILY));
  }

  @Override
  protected void store(byte[] row, byte[] column, byte[] value) throws IOException {
    Put put = new Put(row);
    put.add(QueueEntryRow.COLUMN_FAMILY, column, value);
    hTable.put(put);
    hTable.flushCommits();
  }

  @Override
  protected void store(byte[] row, Map<byte[], byte[]> values) throws IOException {
    if (values.isEmpty()) {
      return;
    }
    Put put = new Put(row);
    for (Map.Entry<byte[], byte[]> entry : values.entrySet()) {
      put.add(QueueEntryRow.COLUMN_FAMILY, entry.getKey(), entry.getValue());
    }
    hTable.put(put);
    hTable.flushCommits();
  }

  @Override
  protected void delete(byte[] row, Set<byte[]> columns) throws IOException {
    if (columns.isEmpty()) {
      return;
    }
    Delete delete = new Delete(row);
    for (byte[] column : columns) {
      delete.deleteColumns(QueueEntryRow.COLUMN_FAMILY, column);
    }
    hTable.delete(delete);
    hTable.flushCommits();
  }
}
