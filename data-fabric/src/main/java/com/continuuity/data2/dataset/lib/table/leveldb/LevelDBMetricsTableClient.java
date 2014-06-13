package com.continuuity.data2.dataset.lib.table.leveldb;

import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.table.Scanner;
import com.continuuity.data2.OperationResult;
import com.continuuity.data2.dataset.lib.table.FuzzyRowFilter;
import com.continuuity.data2.dataset.lib.table.MetricsTable;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import javax.annotation.Nullable;

/**
 * A metrics table client based on leveldb.
 */
public class LevelDBMetricsTableClient implements MetricsTable {

  private final LevelDBOcTableCore core;

  public LevelDBMetricsTableClient(String tableName, LevelDBOcTableService service) throws IOException {
    this.core = new LevelDBOcTableCore(tableName, service);
  }

  @Override
  public OperationResult<byte[]> get(byte[] row, byte[] column) throws Exception {
    NavigableMap<byte[], byte[]> result = core.getRow(row, new byte[][] { column }, null, null, -1, null);
    if (!result.isEmpty()) {
      byte[] value = result.get(column);
      if (value != null) {
        return new OperationResult<byte[]>(value);
      }
    }
    return new OperationResult<byte[]>(StatusCode.KEY_NOT_FOUND);
  }

  @Override
  public void put(Map<byte[], Map<byte[], byte[]>> updates) throws Exception {
    core.persist(updates, System.currentTimeMillis());
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
}
