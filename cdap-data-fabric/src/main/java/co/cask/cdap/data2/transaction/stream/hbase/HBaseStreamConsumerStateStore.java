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
package co.cask.cdap.data2.transaction.stream.hbase;

import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStore;
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
