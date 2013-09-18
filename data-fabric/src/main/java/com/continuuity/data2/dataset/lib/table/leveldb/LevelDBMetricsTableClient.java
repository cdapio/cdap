package com.continuuity.data2.dataset.lib.table.leveldb;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.table.Scanner;
import com.continuuity.data2.dataset.lib.table.FuzzyRowFilter;
import com.continuuity.data2.dataset.lib.table.MetricsTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;

/**
 * A metrics table client based on leveldb.
 */
public class LevelDBMetricsTableClient implements MetricsTable {

  private final LevelDBOcTableCore core;

  public LevelDBMetricsTableClient(LevelDBOcTableCore core) {
    this.core = core;
  }

  @Override
  public void put(Map<byte[], Map<byte[], byte[]>> updates) throws Exception {
    core.persist(updates, System.currentTimeMillis());
  }

  @Override
  public void increment(byte[] row, Map<byte[], Long> increments) throws Exception {
    NavigableMap<byte[], byte[]> existing =
      core.getRow(row, increments.keySet().toArray(new byte[increments.size()][]), null, null, -1, null);
    Map<byte[], byte[]> replacing = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], Long> increment : increments.entrySet()) {
      long existingValue = 0L;
      byte[] existingBytes = existing.get(increment.getKey());
      if (existingBytes != null) {
        if (existingBytes.length != Bytes.SIZEOF_LONG) {
          throw new OperationException(StatusCode.ILLEGAL_INCREMENT,
                                       "Attempted to increment a value that is not convertible to long," +
                                         " row: " + Bytes.toStringBinary(row) +
                                         " column: " + Bytes.toStringBinary(increment.getKey()));
        }
        existingValue = Bytes.toLong(existingBytes);
      }
      replacing.put(increment.getKey(), Bytes.toBytes(existingValue + increment.getValue()));
    }
    put(ImmutableMap.of(row, replacing));
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
  public Scanner scan(@Nullable byte[] start, @Nullable byte[] stop, @Nullable byte[][] columns,
                      @Nullable FuzzyRowFilter filter) {
    return null; // TODO auto generated body
  }
}
