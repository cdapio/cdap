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
package co.cask.cdap.data2.transaction.stream.leveldb;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableCore;
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
public final class LevelDBStreamConsumerStateStore extends StreamConsumerStateStore {

  private final LevelDBTableCore tableCore;

  protected LevelDBStreamConsumerStateStore(StreamConfig streamConfig, LevelDBTableCore tableCore) {
    super(streamConfig);
    this.tableCore = tableCore;
  }

  @Override
  protected byte[] fetch(byte[] row, byte[] column) throws IOException {
    return tableCore.getRow(row, new byte[][] { column }, null, null, -1, null).get(column);
  }

  @Override
  protected void fetchAll(byte[] row, Map<byte[], byte[]> result) throws IOException {
    result.putAll(tableCore.getRow(row, null, null, null, -1, null));
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
    Map<byte[], Map<byte[], byte[]>> changes =
      ImmutableSortedMap.<byte[], Map<byte[], byte[]>>orderedBy(Bytes.BYTES_COMPARATOR)
      .put(row, values)
      .build();

    tableCore.persist(changes, Long.MAX_VALUE);
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
    Map<byte[], Map<byte[], byte[]>> undoes =
      ImmutableSortedMap.<byte[], Map<byte[], byte[]>>orderedBy(Bytes.BYTES_COMPARATOR)
      .put(row, deleteColumns)
      .build();

    tableCore.undo(undoes, Long.MAX_VALUE);
  }

  @Override
  public void close() throws IOException {
    // No-op
  }
}
