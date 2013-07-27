package com.continuuity.data2.dataset.lib.table;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.batch.Split;
import com.continuuity.data.operation.KeyRange;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.table.Scanner;
import com.continuuity.data2.RuntimeTable;
import com.continuuity.data2.dataset.api.DataSetClient;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 *
 */
// should we at least for hbase version cache whatever is fetched from persisted store
public abstract class BufferringOcTableClient implements OrderedColumnarTable, DataSetClient, TransactionAware {
  private static final OperationResult<Map<byte[], byte[]>> EMPTY_RESULT =
    new OperationResult<Map<byte[], byte[]>>(StatusCode.KEY_NOT_FOUND);

  protected static final NavigableMap<byte[], byte[]> EMPTY_ROW_MAP =
    Maps.unmodifiableNavigableMap(Maps.<byte[], byte[], byte[]>newTreeMap(Bytes.BYTES_COMPARATOR));

  private final String name;

  // it will have nulls in place of deleted items
  private NavigableMap<byte[], NavigableMap<byte[], byte[]>> buff;

  public BufferringOcTableClient(String name) {
    this.name = name;
    this.buff = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
  }

  protected String getName() {
    return name;
  }

  protected abstract void persist(NavigableMap<byte[], NavigableMap<byte[], byte[]>> buff)
    throws Exception;

  // never null
  protected abstract NavigableMap<byte[], byte[]> getPersisted(byte[] row)
    throws Exception;

  protected abstract byte[] getPersisted(byte[] row, byte[] column)
    throws Exception;

  // never null
  protected abstract NavigableMap<byte[], byte[]> getPersisted(byte[] row,
                                                               byte[] startColumn, byte[] stopColumn,
                                                               int limit)
    throws Exception;

  // never null
  protected abstract NavigableMap<byte[], byte[]> getPersisted(byte[] row, byte[][] columns)
    throws Exception;


  protected abstract  Scanner scanPersisted(byte[] startRow, byte[] stopRow);

  @Override
  public void close() {
    // DO NOTHING
  }

  @Override
  public void startTx(Transaction tx) {
    buff.clear();
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    // we resolve conflicts on row level of individual table
    List<byte[]> changes = new ArrayList<byte[]>(buff.size());
    for (byte[] changedRow : buff.keySet()) {
      // todo: cache table name in bytes
      changes.add(Bytes.add(Bytes.toBytes(getName()), changedRow));
    }
    return changes;
  }

  @Override
  public boolean commitTx() throws Exception{
    persist(buff);
    buff.clear();
    return true;
  }

  @Override
  public boolean rollbackTx() throws Exception {
    buff.clear();
    return true;
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> get(byte[] row, byte[][] columns) throws Exception {
    // checking if the row was deleted inside this tx
    NavigableMap<byte[], byte[]> buffCols = buff.get(row);
    boolean rowDeleted = buffCols == null && buff.containsKey(row);
    if (rowDeleted) {
      return EMPTY_RESULT;
    }

    // if nothing locally, return all from server
    if (buffCols == null) {
      // todo: put in local? to avoid fetching from server again?
      return createOperationResult(getPersisted(row, columns));
    }

    NavigableMap<byte[], byte[]> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    List<byte[]> colsToFetchFromPersisted = Lists.newArrayList();
    // try to fetch from local buffer first and then from server if it is not in buffer
    for (byte[] column : columns) {
      if (!buffCols.containsKey(column)) {
        colsToFetchFromPersisted.add(column);
        continue;
      }

      byte[] val = buffCols.get(column);
      // if val == null it is a delete, skipping it
      if (val != null) {
        result.put(column, val);
      }
    }

    // fetching from server those that were not found in mem buffer
    if (colsToFetchFromPersisted.size() > 0) {
      NavigableMap<byte[], byte[]> persistedCols =
        getPersisted(row, colsToFetchFromPersisted.toArray(new byte[colsToFetchFromPersisted.size()][]));
      if (persistedCols != null) {
        result.putAll(persistedCols);
      }
    }

    return createOperationResult(result);
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> get(byte[] row, byte[] startColumn, byte[] stopColumn, int limit)
    throws Exception {
    // checking if the row was deleted inside this tx
    NavigableMap<byte[], byte[]> buffCols = buff.get(row);
    boolean rowDeleted = buffCols == null && buff.containsKey(row);
    if (rowDeleted) {
      return EMPTY_RESULT;
    }
    NavigableMap<byte[], byte[]> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    // try to fetch from local buffer first and then from server if it is not in buffer
    NavigableMap<byte[], byte[]> persistedCols = getPersisted(row, startColumn, stopColumn, limit);
    // adding server cols, and then overriding with buffered values
    if (persistedCols != null) {
      result.putAll(persistedCols);
    }

    if (buffCols != null) {
      NavigableMap<byte[], byte[]> buffered;
      if (startColumn == null && stopColumn == null) {
        buffered = buffCols;
      } else if (startColumn == null) {
        buffered = buffCols.headMap(stopColumn, false);
      } else if (stopColumn == null) {
        buffered = buffCols.tailMap(startColumn, true);
      } else {
        buffered = buffCols.subMap(startColumn, true, stopColumn, false);
      }

      addOrDelete(result, buffered);
    }

    // applying limit
    result = head(result, limit);

    return createOperationResult(result);
  }

  private OperationResult<Map<byte[], byte[]>> createOperationResult(NavigableMap<byte[], byte[]> result) {
    if (result == null || result.isEmpty()) {
      return EMPTY_RESULT;
    }
    return new OperationResult<Map<byte[], byte[]>>(result);
  }

  @Override
  public void put(byte[] row, byte[][] columns, byte[][] values) throws Exception {
    NavigableMap<byte[], byte[]> colVals = buff.get(row);
    if (colVals == null) {
      colVals = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      buff.put(row, colVals);
    }
    for (int i = 0; i < columns.length; i++) {
      colVals.put(columns[i], values[i]);
    }

  }

  @Override
  public void delete(byte[] row, byte[][] columns) throws Exception {
    // writing nulls
    put(row, columns, new byte[columns.length][]);
  }

  // todo: why not long?
  @Override
  public Map<byte[], Long> increment(byte[] row, byte[][] columns, long[] amounts) throws Exception {
    NavigableMap<byte[], Long> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    // try to fetch from local buffer first and then from server if it is not in buffer
    // todo: optimize: we need to fetch from server only those that are not available in mem buffer
    NavigableMap<byte[], byte[]> buffCols = buff.get(row);
    if (buffCols == null) {
      buffCols = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      buff.put(row, buffCols);
    }

    // todo: highly inefficient to work with lists
    List<byte[]> colsToFetchFromPersisted = Lists.newArrayList();
    List<Long> amountsToAddToPersisted = Lists.newArrayList();
    // try to fetch from local buffer first and then from server if it is not in buffer
    for (int i = 0; i < columns.length; i++) {
      byte[] column = columns[i];
      if (!buffCols.containsKey(column)) {
        colsToFetchFromPersisted.add(columns[i]);
        amountsToAddToPersisted.add(amounts[i]);
        continue;
      }

      byte[] val = buffCols.get(column);
      // converting to long
      long longVal;
      if (val == null) {
        longVal = 0L;
      } else {
        if (val.length != Bytes.SIZEOF_LONG) {
          throw new OperationException(StatusCode.ILLEGAL_INCREMENT,
                                       "Attempted to increment a value that is not convertible to long," +
                                         " row: " + Bytes.toStringBinary(row) +
                                         " column: " + Bytes.toStringBinary(column));
        }
        longVal = Bytes.toLong(val);
      }
      longVal += amounts[i];
      result.put(column, longVal);
    }


    // fetching from server those that were not found in mem buffer
    if (colsToFetchFromPersisted.size() > 0) {
      NavigableMap<byte[], byte[]> persistedCols =
        getPersisted(row, colsToFetchFromPersisted.toArray(new byte[colsToFetchFromPersisted.size()][]));
      for (int i = 0; i < colsToFetchFromPersisted.size(); i++) {
        byte[] column = colsToFetchFromPersisted.get(i);
        byte[] val = persistedCols.get(column);
        // converting to long
        long longVal;
        if (val == null) {
          longVal = 0L;
        } else {
          if (val.length != Bytes.SIZEOF_LONG) {
            throw new OperationException(StatusCode.ILLEGAL_INCREMENT,
                                         "Attempted to increment a value that is not convertible to long," +
                                           " row: " + Bytes.toStringBinary(row) +
                                           " column: " + Bytes.toStringBinary(column));
          }
          longVal = Bytes.toLong(val);
        }
        longVal += amountsToAddToPersisted.get(i);
        result.put(column, longVal);
      }
    }

    // updating values buffered in memory
    for (Map.Entry<byte[], Long> counter : result.entrySet()) {
      buffCols.put(counter.getKey(), Bytes.toBytes((long) counter.getValue()));
    }

    return result;
  }

  @Override
  public boolean compareAndSwap(byte[] row, byte[] column, byte[] expectedValue, byte[] newValue) throws Exception {
    // checking if the row was deleted inside this tx
    NavigableMap<byte[], byte[]> buffCols = buff.get(row);
    boolean rowDeleted = buffCols == null && buff.containsKey(row);
    byte[] currentValue;
    if (rowDeleted) {
      currentValue = null;
    } else {
      if (buffCols != null && buffCols.containsKey(column)) {
        currentValue = buffCols.get(column);
      } else {
        currentValue = getPersisted(row, column);
      }
    }

    if (Objects.equal(expectedValue, currentValue)) {
      if (buffCols == null) {
        buffCols = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        buff.put(row, buffCols);
      }

      buffCols.put(column, newValue);
      return true;
    }

    return false;
  }

  /**
   * Fallback implementation of getSplits, @see #primitiveGetSplits(). Ideally should be overridden by subclasses
   */
  @Override
  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) {
    List<KeyRange> keyRanges = primitiveGetSplits(numSplits, start, stop);
    return Lists.transform(keyRanges, new Function<KeyRange, Split>() {
      @Nullable
      @Override
      public Split apply(@Nullable KeyRange input) {
        return new RuntimeTable.TableSplit(input.getStart(), input.getStop());
      }
    });
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow) {
    // todo: merge with in-memory buffer
    return scanPersisted(startRow, stopRow);
  }

  // Simplest stateless getSplits method implementation (doesn't use the actual stored data)

  /**
   * If the number of splits is not given, and we have no hints from the table structure (that can be implemented in
   * overriding implementations, though), the primitive getSplits methos will return up to this many splits. Note that
   * we cannot read this number from configuration, because the current OVCTable(Handle) does not pass configuration
   * down into the tables anywhere. See ENG-2395 for the fix.
   */
  static final int DEFAULT_NUMBER_OF_SPLITS = 8;

  /**
   * Simplest possible implementation of getSplits. Takes the given start and end and divides the key space in
   * between into (almost) even partitions, using a long integer approximation of the keys.
   */
  static List<KeyRange> primitiveGetSplits(int numSplits, byte[] start, byte[] stop) {
    // if the range is empty, return no splits
    if (start != null && stop != null && Bytes.compareTo(start, stop) >= 0) {
      return Collections.emptyList();
    }
    if (numSplits <= 0) {
      numSplits = DEFAULT_NUMBER_OF_SPLITS;
    }
    // for simplicity, we construct a long from the begin and end, divide the resulting long range into approximately
    // even splits, and convert the boundaries back to nyte array keys.
    long begin = longForKey(start, false);
    long end = longForKey(stop, true);
    double splitSize = ((double) (end - begin)) / ((double) numSplits);

    // each range will start with the stop key of the previous range.
    // start key of the first range is either the given start, or the least possible key {0};
    List<KeyRange> ranges = Lists.newArrayListWithExpectedSize(numSplits);
    byte[] current = start == null ? new byte[] { 0x00 } : start;
    for (int i = 1; i < numSplits; i++) {
      long bound = begin + (long) (splitSize * i);
      byte[] next = keyForBound(bound);
      // due to rounding and truncation, we may get a bound that is the same as the previous (or if the previous is
      // the start key, less than that). We may also get a bound that exceeds the stop key. In both cases we want to
      // ignore this bound and continue.
      if (Bytes.compareTo(current, next) < 0 && (stop == null || Bytes.compareTo(next, stop) < 0)) {
        ranges.add(new KeyRange(current, next));
        current = next;
      }
    }
    // end of the last range is always the given stop of the range to cover
    ranges.add(new KeyRange(current, stop));

    return ranges;
  }

  // helper method to approximate a row key as a long value. Takes the first 7 bytes from the key and prepends a 0x0;
  // if the key is less than 7 bytes, pads it with zeros to the right.
  static long longForKey(byte[] key, boolean isStop) {
    if (key == null) {
      return isStop ? 0xffffffffffffffL : 0L;
    } else {
      // leading zero helps avoid negative long values for keys beginning with a byte > 0x80
      final byte[] leadingZero = { 0x00 };
      byte[] x;
      if (key.length >= Bytes.SIZEOF_LONG - 1) {
        x = Bytes.add(leadingZero, Bytes.head(key, Bytes.SIZEOF_LONG - 1));
      } else {
        x = Bytes.padTail(Bytes.add(leadingZero, key), Bytes.SIZEOF_LONG - 1 - key.length);
      }
      return Bytes.toLong(x);
    }
  }

  // helper method to convert a long approximation of a long key into a range bound.
  // the following invariant holds: keyForBound(longForKey(key)) == removeTrailingZeros(key).
  // removing the trailing zeros is ok in the context that this is used (only for split bounds)
  // this is called keyForBound on purpose, and not keyForLong.
  static byte[] keyForBound(long value) {
    byte[] bytes = Bytes.tail(Bytes.toBytes(value), Bytes.SIZEOF_LONG - 1);
    int lastNonZero = bytes.length - 1;
    while (lastNonZero > 0 && bytes[lastNonZero] == 0) {
      lastNonZero--;
    }
    return Bytes.head(bytes, lastNonZero + 1);
  }

  // utilities useful for underlying implementations

  protected NavigableMap<byte[], byte[]> getRange(NavigableMap<byte[], byte[]> rowMap,
                                                  byte[] startColumn, byte[] stopColumn,
                                                  int limit) {
    NavigableMap<byte[], byte[]> result;
    if (startColumn == null && stopColumn == null) {
      result = rowMap;
    } else if (startColumn == null) {
      result = rowMap.headMap(stopColumn, false);
    } else if (stopColumn == null) {
      result = rowMap.tailMap(startColumn, true);
    } else {
      result = rowMap.subMap(startColumn, true, stopColumn, false);
    }
    return head(result, limit);
  }

  protected static NavigableMap<byte[], byte[]> head(NavigableMap<byte[], byte[]> map, int count) {
    if (count > 0 && map.size() > count) {
      // todo: is there better way to do it?
      byte [] lastToInclude = null;
      int i = 0;
      for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
        lastToInclude = entry.getKey();
        if (++i >= count) {
          break;
        }
      }
      map = map.headMap(lastToInclude, true);
    }

    return map;
  }

  protected static void addOrDelete(NavigableMap<byte[], byte[]> dest, NavigableMap<byte[], byte[]> src) {
    // value == null means keyval was deleted
    for (Map.Entry<byte[], byte[]> keyVal : src.entrySet()) {
      if (keyVal.getValue() == null) {
        dest.remove(keyVal.getKey());
      } else {
        dest.put(keyVal.getKey(), keyVal.getValue());
      }
    }
  }
}
