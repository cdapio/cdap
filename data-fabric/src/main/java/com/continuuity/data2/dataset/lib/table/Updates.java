package com.continuuity.data2.dataset.lib.table;

import com.continuuity.api.common.Bytes;
import com.continuuity.data2.dataset.lib.table.inmemory.InMemoryOcTableService;
import com.google.common.base.Function;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import javax.annotation.Nullable;

/**
 *
 */
public final class Updates {

  public static final Function<byte[], Update> BYTES_TO_PUTS = new Function<byte[], Update>() {
    @Nullable
    @Override
    public Update apply(@Nullable byte[] input) {
      return new PutValue(input);
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
          currentCell = new TreeMap<Long, byte[]>(InMemoryOcTableService.VERSIONED_VALUE_MAP_COMPARATOR);
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
   * @param base
   * @param modifier
   * @return
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
        if (putBytes.length != Bytes.SIZEOF_LONG) {
          throw new NumberFormatException("Attempted to increment a value that is not convertible to long");
        }

        long newValue = Bytes.toLong(putBytes) + increment.getValue();
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
