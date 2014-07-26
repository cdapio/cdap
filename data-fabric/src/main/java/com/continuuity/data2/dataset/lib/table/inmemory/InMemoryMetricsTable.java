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

package com.continuuity.data2.dataset.lib.table.inmemory;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.table.Scanner;
import com.continuuity.data2.OperationResult;
import com.continuuity.data2.dataset.lib.table.FuzzyRowFilter;
import com.continuuity.data2.dataset.lib.table.MetricsTable;
import com.continuuity.data2.dataset2.lib.table.inmemory.InMemoryOrderedTableService;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import javax.annotation.Nullable;

/**
 * Implements the metrics table API in-memory.
 */
public class InMemoryMetricsTable implements MetricsTable {

  private final String tableName;

  public InMemoryMetricsTable(String name) {
    tableName = name;
  }

  @Override
  public OperationResult<byte[]> get(byte[] row, byte[] column) throws Exception {
    NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap = InMemoryOrderedTableService.get(tableName, row, null);
    if (rowMap != null) {
      NavigableMap<Long, byte[]> valueMap = rowMap.get(column);
      if (valueMap != null && !valueMap.isEmpty()) {
        return new OperationResult<byte[]>(valueMap.firstEntry().getValue());
      }
    }
    return new OperationResult<byte[]>(StatusCode.KEY_NOT_FOUND);
  }

  @Override
  public void put(Map<byte[], Map<byte[], byte[]>> updates) throws Exception {
    InMemoryOrderedTableService.merge(tableName, updates, System.currentTimeMillis());
  }

  @Override
  public boolean swap(byte[] row, byte[] column, byte[] oldValue, byte[] newValue) throws Exception {
    return InMemoryOrderedTableService.swap(tableName, row, column, oldValue, newValue);
  }

  @Override
  public void increment(byte[] row, Map<byte[], Long> increments) throws Exception {
    InMemoryOrderedTableService.increment(tableName, row, increments);
  }

  @Override
  public long incrementAndGet(byte[] row, byte[] column, long delta) throws Exception {
    return InMemoryOrderedTableService.increment(tableName, row, ImmutableMap.of(column, delta)).get(column);
  }

  @Override
  public void deleteAll(byte[] prefix) throws Exception {
    InMemoryOrderedTableService.delete(tableName, prefix);
  }

  @Override
  public void delete(Collection<byte[]> rows) throws Exception {
    InMemoryOrderedTableService.delete(tableName, rows);
  }

  @Override
  public void deleteRange(@Nullable byte[] start, @Nullable byte[] stop, @Nullable byte[][] columns,
                          @Nullable FuzzyRowFilter filter) {
    Scanner scanner = this.scan(start, stop, columns, filter);

    try {
      ImmutablePair<byte[], Map<byte[], byte[]>> rowValues;
      while ((rowValues = scanner.next()) != null) {
        byte[] row = rowValues.getFirst();
        for (byte[] column : rowValues.getSecond().keySet()) {
          InMemoryOrderedTableService.deleteColumns(tableName, row, column);
        }
      }
    } finally {
      scanner.close();
    }
  }

  @Override
  public Scanner scan(@Nullable byte[] start, @Nullable byte[] stop, @Nullable byte[][] columns,
                      @Nullable FuzzyRowFilter filter) {

    // todo: a lot of inefficient copying from one map to another
    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowRange =
      InMemoryOrderedTableService.getRowRange(tableName, start, stop, null);
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> rows = getLatest(rowRange);

    return new InMemoryScanner(rows.entrySet().iterator(), filter, columns);
  }

  private NavigableMap<byte[], NavigableMap<byte[], byte[]>> getLatest(
    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> versionedRows) {
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> rows = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> versionedRow : versionedRows.entrySet()) {
      NavigableMap<byte[], byte[]> columns = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      for (Map.Entry<byte[], NavigableMap<Long, byte[]>> versionedColumn : versionedRow.getValue().entrySet()) {
        columns.put(versionedColumn.getKey(), versionedColumn.getValue().firstEntry().getValue());
      }
      rows.put(versionedRow.getKey(), columns);
    }
    return rows;
  }

  @Override
  public void close() throws IOException {
    // Do nothing
  }
}
