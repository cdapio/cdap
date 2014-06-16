package com.continuuity.data2.dataset.lib.table.inmemory;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.table.Scanner;
import com.continuuity.data2.OperationResult;
import com.continuuity.data2.dataset.lib.table.FuzzyRowFilter;
import com.continuuity.data2.dataset.lib.table.MetricsTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import javax.annotation.Nullable;

/**
 * Implements the metrics table API in-memory.
 */
public class InMemoryMetricsTableClient implements MetricsTable {

  private final String tableName;

  public InMemoryMetricsTableClient(String name) {
    tableName = name;
  }

  @Override
  public OperationResult<byte[]> get(byte[] row, byte[] column) throws Exception {
    NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap = InMemoryOcTableService.get(tableName, row, null);
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
    InMemoryOcTableService.merge(tableName, updates, System.currentTimeMillis());
  }

  @Override
  public boolean swap(byte[] row, byte[] column, byte[] oldValue, byte[] newValue) throws Exception {
    return InMemoryOcTableService.swap(tableName, row, column, oldValue, newValue);
  }

  @Override
  public void increment(byte[] row, Map<byte[], Long> increments) throws Exception {
    InMemoryOcTableService.increment(tableName, row, increments);
  }

  @Override
  public long incrementAndGet(byte[] row, byte[] column, long delta) throws Exception {
    return InMemoryOcTableService.increment(tableName, row, ImmutableMap.of(column, delta)).get(column);
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
  public void deleteRange(@Nullable byte[] start, @Nullable byte[] stop, @Nullable byte[][] columns,
                          @Nullable FuzzyRowFilter filter) {
    Scanner scanner = this.scan(start, stop, columns, filter);

    try {
      ImmutablePair<byte[], Map<byte[], byte[]>> rowValues;
      while ((rowValues = scanner.next()) != null) {
        byte[] row = rowValues.getFirst();
        for (byte[] column : rowValues.getSecond().keySet()) {
          InMemoryOcTableService.deleteColumns(tableName, row, column);
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
        columns.put(versionedColumn.getKey(), versionedColumn.getValue().firstEntry().getValue());
      }
      rows.put(versionedRow.getKey(), columns);
    }
    return rows;
  }

}
