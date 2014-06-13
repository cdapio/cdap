package com.continuuity.data2.dataset.lib.table.inmemory;

import com.continuuity.api.common.Bytes;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import javax.annotation.Nullable;

/**
 *
 */
// todo: use locks instead of synchronize
public class InMemoryOcTableService {
  private static Map<String, ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> tables =
    Maps.newHashMap();

  public static synchronized boolean exists(String tableName) {
    return tables.containsKey(tableName);
  }

  public static synchronized void create(String tableName) {
    if (!tables.containsKey(tableName)) {
      tables.put(tableName, new ConcurrentSkipListMap<byte[],
        NavigableMap<byte[], NavigableMap<Long, byte[]>>>(Bytes.BYTES_COMPARATOR));
    }
  }

  public static synchronized void truncate(String tableName) {
    tables.get(tableName).clear();
  }

  public static synchronized void drop(String tableName) {
    tables.remove(tableName);
  }

  public static synchronized void reset() {
    tables.clear();
  }

  // no nulls
  public static synchronized void merge(String tableName,
                                        Map<byte[], Map<byte[], byte[]>> changes,
                                        long version) {
    ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> table = tables.get(tableName);
    for (Map.Entry<byte[], Map<byte[], byte[]>> change : changes.entrySet()) {
      merge(table, change.getKey(), change.getValue(), version);
    }
  }

  // no nulls
  public static synchronized void merge(String tableName,
                                        NavigableMap<byte[], NavigableMap<byte[], byte[]>> changes,
                                        long version) {
    // todo: handle nulls
    ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> table = tables.get(tableName);
    for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> change : changes.entrySet()) {
      merge(table, change.getKey(), change.getValue(), version);
    }
  }

  private static void merge(ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> table,
                            byte[] row, Map<byte[], byte[]> changes, long version) {
    // get the correct row from the table, create it if it doesn't exist
    NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap = table.get(row);
    if (rowMap == null) {
      rowMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      table.put(row, rowMap);
    }
    // now merge the changes into the row, one by one
    for (Map.Entry<byte[], byte[]> keyVal : changes.entrySet()) {
      // create the column in the row if it does not exist
      NavigableMap<Long, byte[]> colMap = rowMap.get(keyVal.getKey());
      if (colMap == null) {
        colMap = Maps.newTreeMap();
        rowMap.put(keyVal.getKey(), colMap);
      }
      // put into the column with given version
      colMap.put(version, keyVal.getValue());
    }
  }

  public static synchronized Map<byte[], Long> increment(String tableName, byte[] row, Map<byte[], Long> increments) {
    Map<byte[], Long> resultMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> table = tables.get(tableName);
    // get the correct row from the table, create it if it doesn't exist
    NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap = table.get(row);
    if (rowMap == null) {
      rowMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      table.put(row, rowMap);
    }
    // now increment each column, one by one
    long versionForWrite = System.currentTimeMillis();
    for (Map.Entry<byte[], Long> inc : increments.entrySet()) {
      // create the column in the row if it does not exist
      long existingValue;
      NavigableMap<Long, byte[]> colMap = rowMap.get(inc.getKey());
      if (colMap == null) {
        colMap = Maps.newTreeMap();
        rowMap.put(inc.getKey(), colMap);
        existingValue = 0L;
      } else {
        byte[] existingBytes = colMap.lastEntry().getValue();
        if (existingBytes.length != Bytes.SIZEOF_LONG) {
          throw new NumberFormatException("Attempted to increment a value that is not convertible to long," +
                                            " row: " + Bytes.toStringBinary(row) +
                                            " column: " + Bytes.toStringBinary(inc.getKey()));
        }
        existingValue = Bytes.toLong(existingBytes);
      }
      // put into the column with given version
      long newValue = existingValue + inc.getValue();
      resultMap.put(inc.getKey(), newValue);
      colMap.put(versionForWrite, Bytes.toBytes(newValue));
    }
    return resultMap;
  }

  public static synchronized boolean swap(String tableName, byte[] row, byte[] column,
                                          byte[] oldValue, byte[] newValue) {
    ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> table = tables.get(tableName);
    // get the correct row from the table, create it if it doesn't exist
    NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap = table.get(row);
    byte[] existingValue = null;
    if (rowMap != null) {
      NavigableMap<Long, byte[]> columnMap = rowMap.get(column);
      if (columnMap != null) {
        existingValue = columnMap.lastEntry().getValue();
      }
    }
    // verify existing value matches
    if (oldValue == null && existingValue != null) {
      return false;
    }
    if (oldValue != null && (existingValue == null || !Bytes.equals(oldValue, existingValue))) {
      return false;
    }
    // write new value
    if (newValue == null) {
      if (rowMap != null) {
        rowMap.remove(column);
      }
    } else {
      if (rowMap == null) {
        rowMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        table.put(row, rowMap);
      }
      NavigableMap<Long, byte[]> columnMap = rowMap.get(column);
      if (columnMap == null) {
        columnMap = Maps.newTreeMap();
        rowMap.put(column, columnMap);
      }
      columnMap.put(System.currentTimeMillis(), newValue);
    }
    return true;
  }

  public static synchronized void undo(String tableName,
                                       NavigableMap<byte[], NavigableMap<byte[], byte[]>> changes,
                                       long version) {
    // todo: handle nulls
    ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> table = tables.get(tableName);
    for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> change : changes.entrySet()) {
      byte[] row = change.getKey();
      NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap = table.get(row);
      if (rowMap != null) {
        for (byte[] column : change.getValue().keySet()) {
          NavigableMap<Long, byte[]> values = rowMap.get(column);
          values.remove(version);
        }
      }
    }
  }

  public static synchronized void delete(String tableName, Iterable<byte[]> rows) {
    ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> table = tables.get(tableName);
    for (byte[] row : rows) {
      table.remove(row);
    }
  }

  public static synchronized void deleteColumns(String tableName, byte[] row, byte[] column) {
    ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> table = tables.get(tableName);
    NavigableMap<byte[], NavigableMap<Long, byte[]>> columnValues = table.get(row);
    columnValues.remove(column);
  }

  public static synchronized void delete(String tableName, byte[] rowPrefix) {
    ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> table = tables.get(tableName);
    if (rowPrefix.length == 0) {
      table.clear();
    } else {
      byte[] rowAfter = rowAfterPrefix(rowPrefix);
      if (rowAfter == null) {
        table.tailMap(rowPrefix).clear();
      } else {
        table.subMap(rowPrefix, rowAfter).clear();
      }
    }
  }

  /**
   * Given a key prefix, return the smallest key that is greater than all keys starting with that prefix.
   */
  static byte[] rowAfterPrefix(byte[] prefix) {
    Preconditions.checkNotNull("prefix must not be null", prefix);
    for (int i = prefix.length - 1; i >= 0; i--) {
      if (prefix[i] != (byte) 0xff) {
        // i is at the position of the last byte that is not xFF and thus can be incremented
        byte[] after = Arrays.copyOf(prefix, i + 1);
        ++after[i];
        return after;
      }
    }
    // all bytes are xFF -> there is no upper bound
    return null;
  }

  public static synchronized NavigableMap<byte[], NavigableMap<Long, byte[]>> get(String tableName,
                                                                                  byte[] row,
                                                                                  Long version) {
    // todo: handle nulls
    ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> table = tables.get(tableName);
    Preconditions.checkArgument(table != null, "table not found: " + tableName);
    assert table != null;
    NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap = table.get(row);
    return getVisible(rowMap, version);
  }

  public static synchronized NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>
                             getRowRange(String tableName,
                                         byte[] startRow,
                                         byte[] stopRow,
                                         Long version) {
    // todo: handle nulls
    ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> tableData = tables.get(tableName);
    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rows;
    if (startRow == null && stopRow == null) {
      rows = tableData;
    } else if (startRow == null) {
      rows = tableData.headMap(stopRow, false);
    } else if (stopRow == null) {
      rows = tableData.tailMap(startRow, true);
    } else {
      rows = tableData.subMap(startRow, true, stopRow, false);
    }

    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> result =
      Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowMap : rows.entrySet()) {
      result.put(rowMap.getKey(), version == null ? rowMap.getValue() : getVisible(rowMap.getValue(), version));
    }

    return result;
  }

  public static synchronized Collection<String> list() {
    return ImmutableList.copyOf(tables.keySet());
  }

  private static NavigableMap<byte[], NavigableMap<Long, byte[]>> getVisible(
    NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap, Long version) {

    if (rowMap == null) {
      return null;
    }
    NavigableMap<byte[], NavigableMap<Long, byte[]>> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], NavigableMap<Long, byte[]>> column : rowMap.entrySet()) {
      NavigableMap<Long, byte[]> visbleValues = column.getValue();
      if (version != null) {
        visbleValues = visbleValues.headMap(version, true);
      }
      if (visbleValues.size() > 0) {
        NavigableMap<Long, byte[]> colMap = createVersionedValuesMap(visbleValues);
        result.put(column.getKey(), colMap);
      }
    }
    return result;
  }

  private static NavigableMap<Long, byte[]> createVersionedValuesMap(NavigableMap<Long, byte[]> copy) {
    NavigableMap<Long, byte[]> map = Maps.newTreeMap(VERSIONED_VALUE_MAP_COMPARATOR);
    map.putAll(copy);
    return map;
  }

  // This is descending Longs comparator
  private static final Comparator<Long> VERSIONED_VALUE_MAP_COMPARATOR = new Ordering<Long>() {
    @Override
    public int compare(@Nullable Long left, @Nullable Long right) {
      // NOTE: versions never null
      assert left != null && right != null;
      return Longs.compare(right, left);
    }
  };
}
