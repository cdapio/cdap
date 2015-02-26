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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryTableService;
import com.google.common.base.Function;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Utility class for working with {@link Update} instances.
 */
public final class Updates {

  public static final Function<Long, Update> LONG_TO_PUTS = new Function<Long, Update>() {
    @Override
    public Update apply(Long input) {
      return new PutValue(Bytes.toBytes(input));
    }
  };

  /**
   * Returns a new {@code NavigableMap} with the underlying updates represented as {@code byte[]}.
   *
   * <p><emphasis>Note:</emphasis> the map returned is <strong>not thread safe</strong>.</p>
   */
  public static final NavigableMap<byte[], NavigableMap<Long, byte[]>> rowToBytes(
    NavigableMap<byte[], NavigableMap<Long, Update>> row) {
    if (row == null) {
      return null;
    }

    NavigableMap<byte[], NavigableMap<Long, byte[]>> returnMap =
      new TreeMap<byte[], NavigableMap<Long, byte[]>>(Bytes.BYTES_COMPARATOR);
    // TODO: make this use a wrapper to represent the existing map as <Long, byte[]> instead of copying
    for (Map.Entry<byte[], NavigableMap<Long, Update>> entry : row.entrySet()) {
      for (Map.Entry<Long, Update> cellEntry : entry.getValue().entrySet()) {
        NavigableMap<Long, byte[]> currentCell = returnMap.get(entry.getKey());
        if (currentCell == null) {
          currentCell = new TreeMap<Long, byte[]>(InMemoryTableService.VERSIONED_VALUE_MAP_COMPARATOR);
          returnMap.put(entry.getKey(), currentCell);
        }
        byte[] bytes = null;
        if (cellEntry.getValue() != null) {
          bytes = cellEntry.getValue().getBytes();
        }
        currentCell.put(cellEntry.getKey(), bytes);
      }
    }
    return returnMap;
  }

  /**
   * Merges together two Update instances:
   * <ul>
   *   <li>Put a + Put b = Put b</li>
   *   <li>Put a + Increment b = new Put(a + b)</li>
   *   <li>Increment a + Put b = Put b</li>
   *   <li>Increment a + Increment b = new Increment(a + b)</li>
   * </ul>
   * @param base The currently stored or buffered update
   * @param modifier The new update to combine
   * @return A new update combining the base update and modifier, according to the rules above
   */
  public static final Update mergeUpdates(Update base, Update modifier) {
    if (base == null || modifier instanceof PutValue) {
      return modifier;
    }
    if (modifier instanceof IncrementValue) {
      IncrementValue increment = (IncrementValue) modifier;
      if (base instanceof PutValue) {
        PutValue put = (PutValue) base;
        byte[] putBytes = put.getBytes();
        if (putBytes != null && putBytes.length != Bytes.SIZEOF_LONG) {
          throw new NumberFormatException("Attempted to increment a value that is not convertible to long");
        }

        long newValue = (putBytes == null ? 0L : Bytes.toLong(putBytes)) + increment.getValue();
        return new PutValue(Bytes.toBytes(newValue));
      } else if (base instanceof IncrementValue) {
        IncrementValue baseIncrement = (IncrementValue) base;
        return new IncrementValue(baseIncrement.getValue() + increment.getValue());
      }
    }
    // should not happen: modifier is neither Put nor Increment!
    return base;
  }

  /**
   * Returns a new {@link PutValue} representing the update.
   */
  public static final PutValue toPut(Update update) {
    if (update == null) {
      return null;
    } else if (update instanceof PutValue) {
      return (PutValue) update;
    } else if (update instanceof IncrementValue) {
      return new PutValue(((IncrementValue) update).getBytes());
    }
    throw new IllegalArgumentException("Unknown update type " + update);
  }
}
