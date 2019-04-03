/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table.inmemory;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.lib.table.FuzzyRowFilter;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.Update;
import co.cask.cdap.data2.dataset2.lib.table.Updates;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import javax.annotation.Nullable;

/**
 * Implements the metrics table API in-memory.
 */
public class InMemoryMetricsTable implements MetricsTable {

  private final String tableName;

  /**
   * To be used in tests that need namespaces
   */
  public InMemoryMetricsTable(DatasetContext datasetContext, String name, CConfiguration cConf) {
    this(PrefixedNamespaces.namespace(cConf, datasetContext.getNamespaceId(), name));
  }

  /**
   * To be used in tests that do not need namespaces
   */
  @VisibleForTesting
  public InMemoryMetricsTable(String name) {
    tableName = name;
  }

  @Override
  public byte[] get(byte[] row, byte[] column) {
    NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap = InMemoryTableService.get(tableName, row, null);
    if (rowMap != null) {
      NavigableMap<Long, byte[]> valueMap = rowMap.get(column);
      if (valueMap != null && !valueMap.isEmpty()) {
        return valueMap.firstEntry().getValue();
      }
    }
    return null;
  }

  @Override
  public void put(SortedMap<byte[], ? extends SortedMap<byte[], Long>> updates) {
    SortedMap<byte[], SortedMap<byte[], Update>> convertedUpdates = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (NavigableMap.Entry<byte[], ? extends SortedMap<byte[], Long>> entry : updates.entrySet()) {
      convertedUpdates.put(entry.getKey(), Maps.transformValues(entry.getValue(), Updates.LONG_TO_UPDATE));
    }
    InMemoryTableService.merge(tableName, convertedUpdates, System.currentTimeMillis());
  }

  @Override
  public void putBytes(SortedMap<byte[], ? extends SortedMap<byte[], byte[]>> updates) {
    SortedMap<byte[], SortedMap<byte[], Update>> convertedUpdates = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (NavigableMap.Entry<byte[], ? extends SortedMap<byte[], byte[]>> entry : updates.entrySet()) {
      convertedUpdates.put(entry.getKey(), Maps.transformValues(entry.getValue(), Updates.BYTES_TO_UPDATE));
    }
    InMemoryTableService.merge(tableName, convertedUpdates, System.currentTimeMillis());
  }

  @Override
  public boolean swap(byte[] row, byte[] column, byte[] oldValue, byte[] newValue) {
    return InMemoryTableService.swap(tableName, row, column, oldValue, newValue);
  }

  @Override
  public void increment(byte[] row, Map<byte[], Long> increments) {
    InMemoryTableService.increment(tableName, row, increments);
  }

  @Override
  public void increment(NavigableMap<byte[], NavigableMap<byte[], Long>> updates) {
    for (Map.Entry<byte[] , NavigableMap<byte[], Long>> entry : updates.entrySet()) {
      increment(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public long incrementAndGet(byte[] row, byte[] column, long delta) {
    return InMemoryTableService.increment(tableName, row, ImmutableMap.of(column, delta)).get(column);
  }

  @Override
  public void delete(byte[] row, byte[][] columns) {
    for (byte[] column : columns) {
      InMemoryTableService.deleteColumns(tableName, row, column);
    }
  }

  @Override
  public Scanner scan(@Nullable byte[] start, @Nullable byte[] stop,
                      @Nullable FuzzyRowFilter filter) {

    // todo: a lot of inefficient copying from one map to another
    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowRange =
      InMemoryTableService.getRowRange(tableName, start, stop, null);
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> rows = getLatest(rowRange);

    return new InMemoryScanner(rows.entrySet().iterator(), filter, null);
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
  public void close() {
    // Do nothing
  }
}
