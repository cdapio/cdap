package com.continuuity.data2.dataset.lib.table.inmemory;

import com.continuuity.api.common.Bytes;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

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
    tables.put(tableName,
          new ConcurrentSkipListMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>(Bytes.BYTES_COMPARATOR));
  }

  public static synchronized void truncate(String tableName) {
    tables.get(tableName).clear();
  }

  public static synchronized void drop(String tableName) {
    tables.remove(tableName);
  }

  // no nulls
  public static synchronized void merge(String tableName,
                                        NavigableMap<byte[], NavigableMap<byte[], byte[]>> changes,
                                        long version) {
    // todo: handle nulls
    ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> table = tables.get(tableName);
    for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> change : changes.entrySet()) {
      byte[] row = change.getKey();
      NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap = table.get(row);
      if (rowMap == null) {
        rowMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        table.put(row, rowMap);
      }
      // could be done efficiently: we don't need to try delete stuff if we know it was empty. But we don't care :)
      write(rowMap, change.getValue(), version);
    }
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

  public static synchronized NavigableMap<byte[], NavigableMap<Long, byte[]>> get(String tableName,
                                                                                  byte[] row,
                                                                                  long version) {
    // todo: handle nulls
    NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap = tables.get(tableName).get(row);
    return getVisible(rowMap, version);
  }

  public static synchronized NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>
                             getRowRange(String tableName,
                                         byte[] startRow,
                                         byte[] stopRow,
                                         long version) {
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
      result.put(rowMap.getKey(), getVisible(rowMap.getValue(), version));
    }

    return result;
  }

  private static void write(NavigableMap<byte[], NavigableMap<Long, byte[]>> dest,
                           NavigableMap<byte[], byte[]> src,
                           long version) {
    for (Map.Entry<byte[], byte[]> keyVal : src.entrySet()) {
      NavigableMap<Long, byte[]> colMap = dest.get(keyVal.getKey());
      if (colMap == null) {
        colMap = Maps.newTreeMap();
        dest.put(keyVal.getKey(), colMap);
      }

      colMap.put(version, keyVal.getValue());
    }
  }

  private static NavigableMap<byte[], NavigableMap<Long, byte[]>> getVisible(
                                                NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap, long version) {
    if (rowMap == null) {
      return null;
    }
    NavigableMap<byte[], NavigableMap<Long, byte[]>> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], NavigableMap<Long, byte[]>> column : rowMap.entrySet()) {
      NavigableMap<Long, byte[]> visbleValues = column.getValue().headMap(version, true);
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
      return Longs.compare(right, left);
    }
  };
}
