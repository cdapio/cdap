package com.continuuity.data2.dataset.lib.table.inmemory;

import com.continuuity.api.common.Bytes;
import com.continuuity.data.table.Scanner;
import com.continuuity.data2.dataset.lib.table.FuzzyRowFilter;
import com.continuuity.data2.dataset.lib.table.MetricsTable;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Implements the metrics table API in-memory.
 */
public class InMemoryMetricsTableClient implements MetricsTable {

  private final String tableName;

  public InMemoryMetricsTableClient(String name) {
    tableName = name;
  }

  @Override
  public void put(Map<byte[], Map<byte[], byte[]>> updates) throws Exception {
    InMemoryOcTableService.merge(tableName, updates, System.currentTimeMillis());
  }

  @Override
  public void increment(byte[] row, Map<byte[], Long> increments) throws Exception {
    InMemoryOcTableService.increment(tableName, row, increments, System.currentTimeMillis());
  }

  @Override
  public void deleteAll(byte[] prefix) throws Exception {
    InMemoryOcTableService.delete(tableName, prefix);
  }

  @Override
  public void delete(Collection<byte[]> rows) throws Exception {
    InMemoryOcTableService.delete(tableName, rows);
  }

  @Override
  public Scanner scan(@Nullable byte[] start, @Nullable byte[] stop, @Nullable byte[][] columns,
                      @Nullable FuzzyRowFilter filter) {

    // todo: a lot of inefficient copying from one map to another
    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowRange =
      InMemoryOcTableService.getRowRange(tableName, start, stop, null);
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> rows = getLatest(rowRange);

    return new InMemoryScanner(rows.entrySet().iterator(), filter, columns);
  }

  private NavigableMap<byte[], NavigableMap<byte[], byte[]>> getLatest(
    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> versionedRows) {
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> rows = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> versionedRow : versionedRows.entrySet()) {
      NavigableMap<byte[], byte[]> columns = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      for (Map.Entry<byte[], NavigableMap<Long, byte[]>> versionedColumn : versionedRow.getValue().entrySet()) {
        columns.put(versionedColumn.getKey(), versionedColumn.getValue().lastEntry().getValue());
      }
      rows.put(versionedRow.getKey(), columns);
    }
    return rows;
  }

}
