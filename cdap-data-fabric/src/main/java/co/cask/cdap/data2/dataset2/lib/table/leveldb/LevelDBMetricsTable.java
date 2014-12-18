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

package co.cask.cdap.data2.dataset2.lib.table.leveldb;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.data2.dataset2.lib.table.FuzzyRowFilter;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import javax.annotation.Nullable;

/**
 * A metrics table client based on leveldb.
 */
public class LevelDBMetricsTable implements MetricsTable {

  private static final Function<Long, byte[]> LONG_TO_BYTES = new Function<Long, byte[]>() {
    @Override
    public byte[] apply(Long input) {
      return Bytes.toBytes(input);
    }
  };
  private static final Function<NavigableMap<byte[], Long>, NavigableMap<byte[], byte[]>>
    TRANSFORM_MAP_LONG_TO_BYTE_ARRAY = new Function<NavigableMap<byte[], Long>, NavigableMap<byte[], byte[]>>() {
    @Override
    public NavigableMap<byte[], byte[]> apply(NavigableMap<byte[], Long> input) {
      return Maps.transformValues(input, LONG_TO_BYTES);
    }
  };

  private final LevelDBOrderedTableCore core;

  public LevelDBMetricsTable(String tableName, LevelDBOrderedTableService service) throws IOException {
    this.core = new LevelDBOrderedTableCore(tableName, service);
  }

  @Override
  public byte[] get(byte[] row, byte[] column) throws Exception {
    NavigableMap<byte[], byte[]> result = core.getRow(row, new byte[][] { column }, null, null, -1, null);
    if (!result.isEmpty()) {
      return result.get(column);
    }
    return null;
  }

  @Override
  public void put(NavigableMap<byte[], NavigableMap<byte[], Long>> updates) throws Exception {
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> convertedUpdates =
      Maps.transformValues(updates, TRANSFORM_MAP_LONG_TO_BYTE_ARRAY);
    core.persist(convertedUpdates, System.currentTimeMillis());
  }

  @Override
  public synchronized boolean swap(byte[] row, byte[] column, byte[] oldValue, byte[] newValue) throws Exception {
    return core.swap(row, column, oldValue, newValue);
  }

  @Override
  public void increment(byte[] row, Map<byte[], Long> increments) throws Exception {
    core.increment(row, increments);
  }

  @Override
  public void batchIncrement(NavigableMap<byte[], NavigableMap<byte[], Long>> updates) throws Exception {
    core.batchIncrement(updates);
  }

  @Override
  public long incrementAndGet(byte[] row, byte[] column, long delta) throws Exception {
    return core.increment(row, ImmutableMap.of(column, delta)).get(column);
  }

  @Override
  public void deleteAll(byte[] prefix) throws Exception {
    core.deleteRows(prefix);
  }

  @Override
  public void delete(Collection<byte[]> rows) throws Exception {
    core.deleteRows(rows);
  }

  @Override
  public void deleteRange(@Nullable byte[] start, @Nullable byte[] stop, @Nullable byte[][] columns,
                          @Nullable FuzzyRowFilter filter) throws IOException {
    core.deleteRange(start, stop, filter, columns);
  }

  @Override
  public Scanner scan(@Nullable byte[] start, @Nullable byte[] stop, @Nullable byte[][] columns,
                      @Nullable FuzzyRowFilter filter) throws IOException {
    return core.scan(start, stop, filter, columns, null);
  }

  @Override
  public void close() throws IOException {
    // Do nothing
  }

}
