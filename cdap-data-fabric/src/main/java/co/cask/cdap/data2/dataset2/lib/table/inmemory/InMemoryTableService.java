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

package co.cask.cdap.data2.dataset2.lib.table.inmemory;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.data2.dataset2.lib.table.IncrementValue;
import co.cask.cdap.data2.dataset2.lib.table.PutValue;
import co.cask.cdap.data2.dataset2.lib.table.Update;
import co.cask.cdap.data2.dataset2.lib.table.Updates;
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
 * Holds all in-memory tables for {@link InMemoryTable}.
 */
// todo: use locks instead of synchronize
// todo: consider using SortedMap instead of NavigableMap in APIs
public class InMemoryTableService {
  private static Map<String, ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, Update>>>> tables =
    Maps.newHashMap();

  public static synchronized boolean exists(String tableName) {
    return tables.containsKey(tableName);
  }

  public static synchronized void create(String tableName) {
    if (!tables.containsKey(tableName)) {
      tables.put(tableName, new ConcurrentSkipListMap<byte[],
        NavigableMap<byte[], NavigableMap<Long, Update>>>(Bytes.BYTES_COMPARATOR));
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
                                        NavigableMap<byte[], ? extends NavigableMap<byte[], ? extends Update>> changes,
                                        long version) {
    // todo: handle nulls
    ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, Update>>> table = tables.get(tableName);
    NavigableMap<byte[], NavigableMap<byte[], Update>> changesCopy = deepCopyUpdates(changes);
    for (Map.Entry<byte[], NavigableMap<byte[], Update>> change : changesCopy.entrySet()) {
      merge(table, change.getKey(), change.getValue(), version);
    }
  }

  private static void merge(ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, Update>>> table,
                            byte[] row, Map<byte[], Update> changes, long version) {
    // get the correct row from the table, create it if it doesn't exist
    NavigableMap<byte[], NavigableMap<Long, Update>> rowMap = table.get(row);
    if (rowMap == null) {
      rowMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      table.put(row, rowMap);
    }
    // now merge the changes into the row, one by one
    for (Map.Entry<byte[], Update> keyVal : changes.entrySet()) {
      // create the column in the row if it does not exist
      NavigableMap<Long, Update> colMap = rowMap.get(keyVal.getKey());
      if (colMap == null) {
        colMap = Maps.newTreeMap();
        rowMap.put(keyVal.getKey(), colMap);
      }
      // put into the column with given version
      Update merged = Updates.mergeUpdates(colMap.get(version), keyVal.getValue());
      colMap.put(version, merged);
    }
  }

  // todo: remove it from here: only used by "system" metrics table, which should be revised
  @Deprecated
  public static synchronized Map<byte[], Long> increment(String tableName, byte[] row, Map<byte[], Long> increments) {
    Map<byte[], Long> resultMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, Update>>> table = tables.get(tableName);
    // get the correct row from the table, create it if it doesn't exist
    NavigableMap<byte[], NavigableMap<Long, Update>> rowMap = table.get(row);
    if (rowMap == null) {
      rowMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      table.put(row, rowMap);
    }
    // now increment each column, one by one
    long versionForWrite = System.currentTimeMillis();
    for (Map.Entry<byte[], Long> inc : increments.entrySet()) {
      IncrementValue increment = new IncrementValue(inc.getValue());
      // create the column in the row if it does not exist
      NavigableMap<Long, Update> colMap = rowMap.get(inc.getKey());
      Update last = null;
      if (colMap == null) {
        colMap = Maps.newTreeMap();
        rowMap.put(inc.getKey(), colMap);
      } else {
        last = colMap.lastEntry().getValue();
      }
      Update merged = Updates.mergeUpdates(last, increment);
      // put into the column with given version
      long newValue = Bytes.toLong(merged.getBytes());
      resultMap.put(inc.getKey(), newValue);
      colMap.put(versionForWrite, merged);
    }
    return resultMap;
  }

  public static synchronized boolean swap(String tableName, byte[] row, byte[] column,
                                          byte[] oldValue, byte[] newValue) {
    ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, Update>>> table = tables.get(tableName);
    // get the correct row from the table, create it if it doesn't exist
    NavigableMap<byte[], NavigableMap<Long, Update>> rowMap = table.get(row);
    Update existingValue = null;
    if (rowMap != null) {
      NavigableMap<Long, Update> columnMap = rowMap.get(column);
      if (columnMap != null) {
        existingValue = columnMap.lastEntry().getValue();
      }
    }
    // verify existing value matches
    if (oldValue == null && existingValue != null) {
      return false;
    }
    if (oldValue != null && (existingValue == null || !Bytes.equals(oldValue, existingValue.getBytes()))) {
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
      NavigableMap<Long, Update> columnMap = rowMap.get(column);
      if (columnMap == null) {
        columnMap = Maps.newTreeMap();
        rowMap.put(column, columnMap);
      }
      PutValue newPut = new PutValue(newValue);
      columnMap.put(System.currentTimeMillis(), newPut);
    }
    return true;
  }

  public static synchronized void undo(String tableName,
                                       NavigableMap<byte[], NavigableMap<byte[], Update>> changes,
                                       long version) {
    // todo: handle nulls
    ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, Update>>> table = tables.get(tableName);
    for (Map.Entry<byte[], NavigableMap<byte[], Update>> change : changes.entrySet()) {
      byte[] row = change.getKey();
      NavigableMap<byte[], NavigableMap<Long, Update>> rowMap = table.get(row);
      if (rowMap != null) {
        for (byte[] column : change.getValue().keySet()) {
          NavigableMap<Long, Update> values = rowMap.get(column);
          values.remove(version);
        }
      }
    }
  }

  public static synchronized void delete(String tableName, Iterable<byte[]> rows) {
    ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, Update>>> table = tables.get(tableName);
    for (byte[] row : rows) {
      table.remove(row);
    }
  }

  public static synchronized void deleteColumns(String tableName, byte[] row, byte[] column) {
    ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, Update>>> table = tables.get(tableName);
    NavigableMap<byte[], NavigableMap<Long, Update>> columnValues = table.get(row);
    columnValues.remove(column);
  }

  public static synchronized void delete(String tableName, byte[] rowPrefix) {
    ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, Update>>> table = tables.get(tableName);
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
    ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, Update>>> table = tables.get(tableName);
    Preconditions.checkArgument(table != null, "table not found: " + tableName);
    assert table != null;
    NavigableMap<byte[], NavigableMap<Long, Update>> rowMap = table.get(row);
    return deepCopy(Updates.rowToBytes(getVisible(rowMap, version)));
  }

  public static synchronized NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>
                             getRowRange(String tableName,
                                         byte[] startRow,
                                         byte[] stopRow,
                                         Long version) {
    // todo: handle nulls
    ConcurrentNavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, Update>>> tableData = tables.get(tableName);
    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, Update>>> rows;
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
    for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, Update>>> rowMap : rows.entrySet()) {
      NavigableMap<byte[], NavigableMap<Long, Update>> columns =
        version == null ? rowMap.getValue() : getVisible(rowMap.getValue(), version);
      result.put(copy(rowMap.getKey()), deepCopy(Updates.rowToBytes(columns)));
    }

    return result;
  }

  public static synchronized Collection<String> list() {
    return ImmutableList.copyOf(tables.keySet());
  }

  private static NavigableMap<byte[], NavigableMap<Long, Update>> getVisible(
    NavigableMap<byte[], NavigableMap<Long, Update>> rowMap, Long version) {

    if (rowMap == null) {
      return null;
    }
    NavigableMap<byte[], NavigableMap<Long, Update>> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], NavigableMap<Long, Update>> column : rowMap.entrySet()) {
      NavigableMap<Long, Update> visbleValues = column.getValue();
      if (version != null) {
        visbleValues = visbleValues.headMap(version, true);
      }
      if (visbleValues.size() > 0) {
        NavigableMap<Long, Update> colMap = createVersionedValuesMap(visbleValues);
        result.put(column.getKey(), colMap);
      }
    }
    return result;
  }

  private static NavigableMap<Long, Update> createVersionedValuesMap(NavigableMap<Long, Update> copy) {
    NavigableMap<Long, Update> map = Maps.newTreeMap(VERSIONED_VALUE_MAP_COMPARATOR);
    map.putAll(copy);
    return map;
  }

  @Nullable
  private static NavigableMap<byte[], NavigableMap<byte[], Update>> deepCopyUpdates(
    @Nullable NavigableMap<byte[], ? extends NavigableMap<byte[], ? extends Update>> src) {

    if (src == null) {
      return null;
    }

    NavigableMap<byte[], NavigableMap<byte[], Update>> copy = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], ? extends NavigableMap<byte[], ? extends Update>> entry : src.entrySet()) {
      byte[] key = copy(entry.getKey());
      NavigableMap<byte[], Update> columnUpdates = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      copy.put(key, columnUpdates);
      for (Map.Entry<byte[], ? extends Update> updateEntry : entry.getValue().entrySet()) {
        byte[] col = copy(updateEntry.getKey());
        columnUpdates.put(col, updateEntry.getValue().deepCopy());
      }
    }

    return copy;
  }

  @Nullable
  private static NavigableMap<byte[], NavigableMap<Long, byte[]>> deepCopy(
    @Nullable NavigableMap<byte[], NavigableMap<Long, byte[]>> src) {

    if (src == null) {
      return null;
    }

    NavigableMap<byte[], NavigableMap<Long, byte[]>> copy = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], NavigableMap<Long, byte[]>> entry : src.entrySet()) {
      byte[] key = copy(entry.getKey());
      NavigableMap<Long, byte[]> columnValues = Maps.newTreeMap(VERSIONED_VALUE_MAP_COMPARATOR);
      copy.put(key, columnValues);
      for (Map.Entry<Long, byte[]> valueEntry : entry.getValue().entrySet()) {
        columnValues.put(valueEntry.getKey(), copy(valueEntry.getValue()));
      }
    }

    return copy;
  }

  @Nullable
  private static byte[] copy(@Nullable byte[] src) {
    return src == null ? null : Arrays.copyOf(src, src.length);
  }

  // This is descending Longs comparator
  public static final Comparator<Long> VERSIONED_VALUE_MAP_COMPARATOR = new Ordering<Long>() {
    @Override
    public int compare(@Nullable Long left, @Nullable Long right) {
      // NOTE: versions never null
      assert left != null && right != null;
      return Longs.compare(right, left);
    }
  };
}
