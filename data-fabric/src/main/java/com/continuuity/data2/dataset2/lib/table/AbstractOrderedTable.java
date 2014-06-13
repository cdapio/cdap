package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.table.OrderedTable;
import com.google.common.collect.ImmutableSortedMap;

import java.util.Map;
import java.util.NavigableMap;

/**
 * Implements some of the methods in a generic way (not necessarily in most efficient way).
 */
public abstract class AbstractOrderedTable implements OrderedTable {
  // empty immutable row's column->value map constant
  // Using ImmutableSortedMap instead of Maps.unmodifiableNavigableMap to avoid conflicts with
  // Hadoop, which uses an older version of guava without that method.
  protected static final NavigableMap<byte[], byte[]> EMPTY_ROW_MAP =
    ImmutableSortedMap.<byte[], byte[]>orderedBy(Bytes.BYTES_COMPARATOR).build();

  @Override
  public byte[] get(byte[] row, byte[] column) throws Exception {
    Map<byte[], byte[]> result = get(row, new byte[][]{column});
    return result.isEmpty() ? null : result.get(column);
  }

  @Override
  public void put(byte [] row, byte [] column, byte[] value) throws Exception {
    put(row, new byte[][] {column}, new byte[][] {value});
  }

  @Override
  public long increment(byte[] row, byte[] column, long amount) throws Exception {
    return increment(row, new byte[][] {column}, new long[] {amount}).get(column);
  }

  @Override
  public void delete(byte[] row, byte[] column) throws Exception {
    delete(row, new byte[][] {column});
  }
}
