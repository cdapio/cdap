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

package co.cask.cdap.data2.transaction.stream.inmemory;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryTable;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStore;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public final class InMemoryStreamConsumerStateStore extends StreamConsumerStateStore {

  private final InMemoryTable table;

  protected InMemoryStreamConsumerStateStore(StreamConfig streamConfig, InMemoryTable table) {
    super(streamConfig);
    this.table = table;
  }

  @Override
  protected byte[] fetch(byte[] row, byte[] column) throws IOException {
    try {
      return table.get(row, column);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void fetchAll(byte[] row, Map<byte[], byte[]> result) throws IOException {
    try {
      Row fetched = table.get(row);
      if (!fetched.isEmpty()) {
        result.putAll(fetched.getColumns());
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void fetchAll(byte[] row, byte[] columnPrefix, Map<byte[], byte[]> result) throws IOException {
    // Ignore the column prefix. Parent class would handle it.
    fetchAll(row, result);
  }

  @Override
  protected void store(byte[] row, byte[] column, byte[] value) throws IOException {
    store(row, ImmutableSortedMap.<byte[], byte[]>orderedBy(Bytes.BYTES_COMPARATOR).put(column, value).build());
  }

  @Override
  protected void store(byte[] row, Map<byte[], byte[]> values) throws IOException {
    if (values.isEmpty()) {
      return;
    }
    byte[][] columns = new byte[values.size()][];
    byte[][] vals = new byte[values.size()][];
    int i = 0;
    for (Map.Entry<byte[], byte[]> columnValue : values.entrySet()) {
      columns[i] = columnValue.getKey();
      vals[i] = columnValue.getValue().length > 0 ? columnValue.getValue() : null;
      i++;
    }

    try {
      table.put(row, columns, vals);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void delete(byte[] row, Set<byte[]> columns) throws IOException {
    if (columns.isEmpty()) {
      return;
    }
    Map<byte[], byte[]> deleteColumns = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (byte[] column : columns) {
      deleteColumns.put(column, Bytes.EMPTY_BYTE_ARRAY);   // Value doesn't matter
    }
    byte[][] columnsArr = new byte[columns.size()][];
    int i = 0;
    for (byte[] column : columns) {
      columnsArr[i] = column;
      i++;
    }

    try {
      table.delete(row, columnsArr);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    // No-op
  }
}
