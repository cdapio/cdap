/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.engine.memory;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.table.AbstractOVCTable;
import com.continuuity.data.table.Scanner;
import com.continuuity.data.util.RowLockTable;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * An in-memory implementation of a column-oriented table similar to HBase.
 * <p/>
 * A row has one or more columns, and a column has one or more versions.
 * <p/>
 * Columns are sorted in ascending binary order, versions of a column are sorted
 * in descending timestamp order.
 * <p/>
 * This version of MemoryTable is currently NOT sorted by row.
 */
public class MemoryOVCTable extends AbstractOVCTable {

  private final byte[] name;

  private final ConcurrentNavigableMap<RowLockTable.Row, // row to
    NavigableMap<Column, // column to
      NavigableMap<Version, Value>>> map = // version to value
    new ConcurrentSkipListMap<RowLockTable.Row, NavigableMap<Column, NavigableMap<Version, Value>>>();

  // this will be use to implement row-level locking
  private final RowLockTable locks = new RowLockTable();

  public MemoryOVCTable(final byte[] tableName) {
    this.name = tableName;
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public void put(byte[] row, byte[] column, long version, byte[] value) {
    put(row, new byte[][]{column}, version, new byte[][]{value});
  }

  @Override
  public void put(byte[] row, byte[][] columns, long version, byte[][] values) {
    assert (columns.length == values.length);
    RowLockTable.Row r = new RowLockTable.Row(row);
    NavigableMap<Column, NavigableMap<Version, Value>> map = getAndLockRow(r);
    try {
      for (int i = 0; i < columns.length; i++) {
        NavigableMap<Version, Value> columnMap = getColumn(map, columns[i]);
        columnMap.put(new Version(version), new Value(values[i]));
      }
    } finally {
      this.locks.unlock(r);
    }
  }

  @Override
  public void put(byte[][] rows, byte[][] columns, long version, byte[][] values) throws OperationException {
    assert (columns.length == values.length);
    assert (rows.length == columns.length);
    for (int i = 0; i < rows.length; i++) {
      RowLockTable.Row r = new RowLockTable.Row(rows[i]);
      NavigableMap<Column, NavigableMap<Version, Value>> map = getAndLockRow(r);
      try {
        NavigableMap<Version, Value> columnMap = getColumn(map, columns[i]);
        columnMap.put(new Version(version), new Value(values[i]));
      } finally {
        this.locks.unlock(r);
      }
    }
  }

  @Override
  public void put(byte[][] rows, byte[][][] columnsPerRow, long version, byte[][][] valuesPerRow)
    throws OperationException {
    for (int i = 0; i < rows.length; i++) {
      this.put(rows[i], columnsPerRow[i], version, valuesPerRow[i]);
    }
  }

  @Override
  public void delete(byte[] row, byte[] column, long version) {
    performDelete(row, column, version, Version.Type.DELETE);
  }

  @Override
  public void delete(byte[] row, byte[][] columns, long version) {
    performDelete(row, columns, version, Version.Type.DELETE);
  }

  @Override
  public void deleteAll(byte[] row, byte[] column, long version) {
    performDelete(row, column, version, Version.Type.DELETE_ALL);
  }

  @Override
  public void deleteAll(byte[] row, byte[][] columns, long version) {
    performDelete(row, columns, version, Version.Type.DELETE_ALL);
  }

  @Override
  public void undeleteAll(byte[] row, byte[] column, long version) {
    performDelete(row, column, version, Version.Type.UNDELETE_ALL);
  }

  @Override
  public void undeleteAll(byte[] row, byte[][] columns, long version) {
    performDelete(row, columns, version, Version.Type.UNDELETE_ALL);
  }

  private void performDelete(byte[] row, byte[] column, long version, Version.Type type) {
    performDelete(row, new byte[][]{column}, version, type);
  }

  private void performDelete(byte[] row, byte[][] columns, long version, Version.Type type) {
    RowLockTable.Row r = new RowLockTable.Row(row);
    NavigableMap<Column, NavigableMap<Version, Value>> map = getAndLockRow(r);
    try {
      for (byte[] column : columns) {
        NavigableMap<Version, Value> columnMap = getColumn(map, column);
        columnMap.put(new Version(version, type), Value.delete());
      }
    } finally {
      this.locks.unlock(r);
    }
  }

  @Override
  public void deleteDirty(byte[] row, byte[][] columns, long version) {
    RowLockTable.Row r = new RowLockTable.Row(row);
    NavigableMap<Column, NavigableMap<Version, Value>> map = getAndLockRow(r);
    try {
      deleteDirtyNoLock(map, columns, version);
    } finally {
      if (map.isEmpty()) {
        // this row is gone, remove it from the table and also from the lock table
        this.map.remove(r); // safe to remove because we have the lock
        this.locks.unlockAndRemove(r); // now remove, invalidate and unlock the lock
      } else {
        this.locks.unlock(r);
      }
    }
  }

  private void deleteDirtyNoLock(NavigableMap<Column, NavigableMap<Version, Value>> rowMap, byte[][] columns,
                                 long version) {
    for (byte[] column : columns) {
      NavigableMap<Version, Value> columnMap = getColumn(rowMap, column);
      while (!columnMap.isEmpty() && columnMap.lastKey().stamp <= version) {
        columnMap.pollLastEntry();
      }
      if (columnMap.isEmpty()) {
        rowMap.remove(new Column(column));
      }
    }
  }

  @Override
  public void deleteDirty(byte[][] rows) throws OperationException {
    for (byte[] row : rows) {
      RowLockTable.Row r = new RowLockTable.Row(row);
      try {
        // this row is gone, remove it from the table and also from the lock table
        this.map.remove(r); // safe to remove because we have the lock
      } finally {
        this.locks.removeLock(r); // now remove, invalidate and unlock the lock
      }
    }
  }

  @Override
  public void deleteRowsDirtily(byte[] startRow, byte[] stopRow) throws OperationException {
    Preconditions.checkNotNull(startRow, "start row cannot be null");
    getRowRange(startRow, stopRow).clear();
    locks.removeRange(new RowLockTable.Row(startRow), stopRow == null ? null : new RowLockTable.Row(stopRow));
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> get(byte[] row, ReadPointer pointer) {

    RowLockTable.Row r = new RowLockTable.Row(row);
    NavigableMap<Column, NavigableMap<Version, Value>> map = getAndLockExistingRow(r);
    if (map == null) {
      return new OperationResult<Map<byte[], byte[]>>(StatusCode.KEY_NOT_FOUND);
    }
    try {
      Map<byte[], byte[]> ret = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);

      for (Map.Entry<Column, NavigableMap<Version, Value>> entry : map.entrySet()) {
        ImmutablePair<Long, byte[]> latest = filteredLatest(entry.getValue(), pointer);
        if (latest == null) {
          continue;
        }
        ret.put(entry.getKey().getValue(), latest.getSecond());
      }

      if (ret.isEmpty()) {
        return new OperationResult<Map<byte[], byte[]>>(StatusCode.KEY_NOT_FOUND);
      }
      return new OperationResult<Map<byte[], byte[]>>(ret);

    } finally {
      this.locks.unlock(r);
    }
  }

  @Override
  public OperationResult<byte[]> get(byte[] row, byte[] column, ReadPointer readPointer) {

    RowLockTable.Row r = new RowLockTable.Row(row);
    NavigableMap<Column, NavigableMap<Version, Value>> map = getAndLockExistingRow(r);
    if (map == null) {
      return new OperationResult<byte[]>(StatusCode.KEY_NOT_FOUND);
    }
    try {
      NavigableMap<Version, Value> columnMap = getColumn(map, column);
      ImmutablePair<Long, byte[]> latest = filteredLatest(columnMap, readPointer);

      if (latest == null) {
        return new OperationResult<byte[]>(StatusCode.COLUMN_NOT_FOUND);
      } else {
        return new OperationResult<byte[]>(latest.getSecond());
      }
    } finally {
      this.locks.unlock(r);
    }
  }

  @Override
  public OperationResult<ImmutablePair<byte[], Long>> getWithVersion(byte[] row, byte[] column,
                                                                     ReadPointer readPointer) {

    RowLockTable.Row r = new RowLockTable.Row(row);
    NavigableMap<Column, NavigableMap<Version, Value>> map = getAndLockExistingRow(r);
    if (map == null) {
      return new OperationResult<ImmutablePair<byte[], Long>>(StatusCode.KEY_NOT_FOUND);
    }
    try {
      NavigableMap<Version, Value> columnMap = getColumn(map, column);
      ImmutablePair<Long, byte[]> latest = filteredLatest(columnMap, readPointer);

      if (latest == null) {
        return new OperationResult<ImmutablePair<byte[], Long>>(StatusCode.COLUMN_NOT_FOUND);
      } else {
        return new OperationResult<ImmutablePair<byte[], Long>>(new ImmutablePair<byte[], Long>(latest.getSecond(),
                                                                                                latest.getFirst()));
      }
    } finally {
      this.locks.unlock(r);
    }
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> get(byte[] row, byte[][] columns, ReadPointer readPointer) {

    RowLockTable.Row r = new RowLockTable.Row(row);
    NavigableMap<Column, NavigableMap<Version, Value>> map = getAndLockExistingRow(r);
    if (map == null) {
      return new OperationResult<Map<byte[], byte[]>>(StatusCode.KEY_NOT_FOUND);
    }
    try {
      Map<byte[], byte[]> ret = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
      for (byte[] column : columns) {
        NavigableMap<Version, Value> columnMap = getColumn(map, column);
        ImmutablePair<Long, byte[]> latest = filteredLatest(columnMap, readPointer);
        if (latest != null) {
          ret.put(column, latest.getSecond());
        }
      }
      if (ret.isEmpty()) {
        return new OperationResult<Map<byte[], byte[]>>(StatusCode.COLUMN_NOT_FOUND);
      } else {
        return new OperationResult<Map<byte[], byte[]>>(ret);
      }
    } finally {
      this.locks.unlock(r);
    }
  }

  @Override
  public OperationResult<byte[]> getDirty(byte[] row, byte[] column) throws OperationException {
    return get(row, column, TransactionOracle.DIRTY_READ_POINTER);
  }

  @Override
  public OperationResult<Map<byte[], Map<byte[], byte[]>>> getAllColumns(byte[][] rows, byte[][] columns,
                                                                         ReadPointer readPointer)
    throws OperationException {
    Map<byte[], Map<byte[], byte[]>> ret = new TreeMap<byte[], Map<byte[], byte[]>>(Bytes.BYTES_COMPARATOR);
    for (byte[] row : rows) {
      Map<byte[], byte[]> writeColumnMap = ret.get(row);
      if (writeColumnMap == null) {
        writeColumnMap = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
        ret.put(row, writeColumnMap);
      }
      RowLockTable.Row r = new RowLockTable.Row(row);
      NavigableMap<Column, NavigableMap<Version, Value>> readRowMap = getAndLockExistingRow(r);
      if (readRowMap == null) {
        continue;
      }
      try {
        for (byte[] column : columns) {
          NavigableMap<Version, Value> readColumnMap = getColumn(readRowMap, column);
          ImmutablePair<Long, byte[]> latest = filteredLatest(readColumnMap, readPointer);
          if (latest != null) {
            writeColumnMap.put(column, latest.getSecond());
          }
        }
      } finally {
        this.locks.unlock(r);
      }
    }
    // Remove empty rows
    for (Iterator<Entry<byte[], Map<byte[], byte[]>>> iterator = ret.entrySet().iterator(); iterator.hasNext(); ) {
      if (iterator.next().getValue().isEmpty()) {
        iterator.remove();
      }
    }
    if (ret.isEmpty()) {
      return new OperationResult<Map<byte[], Map<byte[], byte[]>>>(StatusCode.KEY_NOT_FOUND);
    } else {
      return new OperationResult<Map<byte[], Map<byte[], byte[]>>>(ret);
    }
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> get(byte[] row, byte[] startColumn, byte[] stopColumn, int limit,
                                                  ReadPointer readPointer) {

    // negative limit means unlimited results
    if (limit <= 0) {
      limit = Integer.MAX_VALUE;
    }

    RowLockTable.Row r = new RowLockTable.Row(row);
    NavigableMap<Column, NavigableMap<Version, Value>> map = getAndLockExistingRow(r);
    if (map == null) {
      return new OperationResult<Map<byte[], byte[]>>(StatusCode.KEY_NOT_FOUND);
    }
    try {
      Map<byte[], byte[]> ret = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);

      SortedMap<Column, NavigableMap<Version, Value>> sub;
      if (isEmpty(startColumn) && isEmpty(stopColumn)) {
        sub = map;
      } else if (isEmpty(startColumn)) {
        sub = map.headMap(new Column(stopColumn));
      } else if (isEmpty(stopColumn)) {
        sub = map.tailMap(new Column(startColumn));
      } else {
        sub = map.subMap(new Column(startColumn), new Column(stopColumn));
      }

      for (Map.Entry<Column, NavigableMap<Version, Value>> entry : sub.entrySet()) {
        NavigableMap<Version, Value> columnMap = entry.getValue();
        ImmutablePair<Long, byte[]> latest = filteredLatest(columnMap, readPointer);
        if (latest != null) {
          ret.put(entry.getKey().getValue(), latest.getSecond());
          // break out if limit reached
          if (ret.size() >= limit) {
            break;
          }
        }
      }
      if (ret.isEmpty()) {
        return new OperationResult<Map<byte[], byte[]>>(StatusCode.COLUMN_NOT_FOUND);
      } else {
        return new OperationResult<Map<byte[], byte[]>>(ret);
      }
    } finally {
      this.locks.unlock(r);
    }
  }


  private boolean isEmpty(byte[] column) {
    return column == null || column.length == 0;
  }

  @Override
  public List<byte[]> getKeys(int limit, int offset, ReadPointer readPointer) {
    List<byte[]> keys = new ArrayList<byte[]>(limit > 1024 ? 1024 : limit);
    int returned = 0;
    int skipped = 0;

    for (Map.Entry<RowLockTable.Row, NavigableMap<Column, NavigableMap<Version, Value>>> entry : this.map.entrySet()) {
      // Determine if row is visible
      if (hasAnyVisibleColumns(entry.getValue(), readPointer)) {
        if (skipped < offset) {
          skipped++;
        } else if (returned < limit) {
          returned++;
          keys.add(entry.getKey().getValue());
        }
        if (returned == limit) {
          return keys;
        }
      }
    }
    return keys;
  }

  private boolean hasAnyVisibleColumns(NavigableMap<Column, NavigableMap<Version, Value>> columns,
                                       ReadPointer readPointer) {
    for (Map.Entry<Column, NavigableMap<Version, Value>> entry : columns.entrySet()) {
      if (filteredLatest(entry.getValue(), readPointer) != null) {
        return true;
      }
    }
    return false;
  }

  private NavigableMap<RowLockTable.Row, NavigableMap<Column, NavigableMap<Version, Value>>>
  getRowRange(byte[] startRow, byte[] stopRow) {
    if (startRow == null && stopRow == null) {
      return this.map;
    } else if (startRow == null) {
      return this.map.headMap(new RowLockTable.Row(stopRow));
    } else if (stopRow == null) {
      return this.map.tailMap(new RowLockTable.Row(startRow));
    } else {
      return this.map.subMap(new RowLockTable.Row(startRow), new RowLockTable.Row(stopRow));
    }
  }

  @Override
  public Scanner scan(ReadPointer readPointer) {
    return new MemoryScanner(this.map.entrySet().iterator(), readPointer);
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow, ReadPointer readPointer) {
    return new MemoryScanner(getRowRange(startRow, stopRow).entrySet().iterator(), readPointer);
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow, byte[][] columns, ReadPointer readPointer) {
    return new MemoryScanner(getRowRange(startRow, stopRow).entrySet().iterator(), readPointer);
  }

  /**
   * An in-memory implememtation of a scanner.
   */
  public class MemoryScanner implements Scanner {

    private final Iterator<Entry<RowLockTable.Row, NavigableMap<Column, NavigableMap<Version, Value>>>> rows;

    private final ReadPointer readPointer;

    private final Set<byte[]> columnSet;

    public MemoryScanner(Iterator<Entry<RowLockTable.Row, NavigableMap<Column, NavigableMap<Version, Value>>>> rows,
                         ReadPointer readPointer) {
      this(rows, null, readPointer);
    }

    public MemoryScanner(Iterator<Entry<RowLockTable.Row, NavigableMap<Column, NavigableMap<Version, Value>>>> rows,
                         byte[][] columns, ReadPointer readPointer) {
      this.rows = rows;
      this.readPointer = readPointer;
      this.columnSet = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
      if (columns != null) {
        Collections.addAll(this.columnSet, columns);
      }
    }

    @Override
    public ImmutablePair<byte[], Map<byte[], byte[]>> next() {
      Map<byte[], byte[]> columns = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
      Entry<RowLockTable.Row, NavigableMap<Column, NavigableMap<Version, Value>>> rowEntry = null;
      boolean gotNext = false;

      while (!gotNext){
        if (!this.rows.hasNext()){
          break;
        }
        rowEntry = this.rows.next();
        //Try to read all columns for this row
        for (Map.Entry<Column, NavigableMap<Version, Value>> colEntry : rowEntry.getValue().entrySet()) {
          if (!this.columnSet.isEmpty() && !this.columnSet.contains(colEntry.getKey().getValue())) {
            continue;
          }
          ImmutablePair<Long, byte[]> latest = filteredLatest(colEntry.getValue(), readPointer);
          if (latest != null){
            columns.put(colEntry.getKey().getValue(), latest.getSecond());
          }
        }
        if (columns.size() > 0) {
          //there is alteast one valid col for row. Exit the loop. If not try next row
          gotNext =  true;
        }
      }
      if (columns.size() > 0) {
        return new ImmutablePair<byte[], Map<byte[], byte[]>>(rowEntry.getKey().getValue(), columns);
      } else {
        return null;
      }
    }

    @Override
    public void close() { /* no op */ }

  }

  @Override
  public long increment(byte[] row, byte[] column, long amount, ReadPointer readPointer, long writeVersion)
    throws OperationException {
    return increment(row, new byte[][]{column}, new long[]{amount}, readPointer, writeVersion).get(column);
  }

  @Override
  public Map<byte[], Long> increment(byte[] row, byte[][] columns, long[] amounts, ReadPointer readPointer,
                                     long writeVersion)
    throws OperationException {

    RowLockTable.Row r = new RowLockTable.Row(row);
    NavigableMap<Column, NavigableMap<Version, Value>> map = getAndLockRow(r);

    long[] newAmounts = new long[amounts.length];
    Map<byte[], Long> newAmountsMap = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);

    try {
      // first determine new values for all columns. This can thrown an
      // exception if an existing value is not sizeof(long).
      for (int i = 0; i < columns.length; i++) {
        byte[] column = columns[i];
        long amount = amounts[i];
        NavigableMap<Version, Value> versions = getColumn(map, column);
        long existingAmount = 0L;
        ImmutablePair<Long, byte[]> latest = filteredLatest(versions, readPointer);
        if (latest != null) {
          try {
            existingAmount = Bytes.toLong(latest.getSecond());
          } catch (IllegalArgumentException e) {
            throw new OperationException(StatusCode.ILLEGAL_INCREMENT, e.getMessage(), e);
          }
        }
        long newAmount = existingAmount + amount;
        newAmounts[i] = newAmount;
      }
      // now we know all values are legal, we can apply all increments
      for (int i = 0; i < columns.length; i++) {
        byte[] column = columns[i];
        NavigableMap<Version, Value> versions = getColumn(map, column);
        versions.put(new Version(writeVersion), new Value(Bytes.toBytes(newAmounts[i])));
        newAmountsMap.put(column, newAmounts[i]);
      }
      return newAmountsMap;

    } finally {
      this.locks.unlock(r);
    }
  }

  @Override
  public long incrementAtomicDirtily(byte[] row, byte[] column, long amount) throws OperationException {
    RowLockTable.Row r = new RowLockTable.Row(row);
    NavigableMap<Column, NavigableMap<Version, Value>> map = getAndLockRow(r);

    long newAmount;
    try {
      // first determine new values for the column. This can thrown an
      // exception if an existing value is not sizeof(long).
      NavigableMap<Version, Value> versions = getColumn(map, column);
      long existingAmount = 0L;
      ImmutablePair<Long, byte[]> latest = latest(versions);
      if (latest != null) {
        try {
          existingAmount = Bytes.toLong(latest.getSecond());
        } catch (IllegalArgumentException e) {
          throw new OperationException(StatusCode.ILLEGAL_INCREMENT, e.getMessage(), e);
        }
      }
      newAmount = existingAmount + amount;
      // now we know all values are legal, we can apply all increments
      versions.put(new Version(TransactionOracle.DIRTY_WRITE_VERSION), new Value(Bytes.toBytes(newAmount)));
      return newAmount;

    } finally {
      this.locks.unlock(r);
    }
  }

  @Override
  public void compareAndSwap(byte[] row, byte[] column, byte[] expectedValue, byte[] newValue,
                             ReadPointer readPointer, long writeVersion)
    throws OperationException {

    RowLockTable.Row r = new RowLockTable.Row(row);
    NavigableMap<Column, NavigableMap<Version, Value>> map = getAndLockRow(r);
    try {
      NavigableMap<Version, Value> columnMap = getColumn(map, column);
      ImmutablePair<Long, byte[]> latest = filteredLatest(columnMap, readPointer);
      if (latest == null && expectedValue != null) {
        throw new OperationException(StatusCode.WRITE_CONFLICT, "CompareAndSwap expected value mismatch");
      }
      if (latest != null) {
        if (expectedValue == null) {
          throw new OperationException(StatusCode.WRITE_CONFLICT, "CompareAndSwap expected value mismatch");
        } else if (!Bytes.equals(latest.getSecond(), expectedValue)) {
          throw new OperationException(StatusCode.WRITE_CONFLICT, "CompareAndSwap expected value mismatch");
        }
      }
      if (newValue == null || newValue.length == 0) {
        columnMap.put(new Version(writeVersion, Version.Type.DELETE_ALL), Value.delete());
      } else {
        columnMap.put(new Version(writeVersion), new Value(newValue));
      }
    } finally {
      this.locks.unlock(r);
    }
  }

  @Override
  public boolean compareAndSwapDirty(byte[] row, byte[] column, byte[] expectedValue, byte[] newValue)
    throws OperationException {
    RowLockTable.Row r = new RowLockTable.Row(row);
    NavigableMap<Column, NavigableMap<Version, Value>> map = getAndLockRow(r);

    try {
      // Read the exising value at the row, col. Note: we are assuming there is no tombstone!
      NavigableMap<Version, Value> columnMap = getColumn(map, column);
      byte[] oldValue = null;
      ImmutablePair<Long, byte[]> latest = latest(columnMap);
      if (latest != null) {
        oldValue = latest.getSecond();
      }

      // expectedValue is equal to oldValue, do the swap
      if ((oldValue == null && expectedValue == null) || Bytes.equals(oldValue, expectedValue)) {
        // if newValue is null, delete
        if (newValue == null || newValue.length == 0) {
          deleteDirtyNoLock(map, new byte[][]{column}, TransactionOracle.DIRTY_WRITE_VERSION);
        } else {
          columnMap.put(new Version(TransactionOracle.DIRTY_WRITE_VERSION), new Value(newValue));
        }
        return true;
      }

      return false;
    } finally {
      this.locks.unlock(r);
    }
  }

  // Private helpers

  /**
   * Makes a copy of all visible versions of columns within the specified column
   * map, filtering out deleted values and everything with a version higher than
   * the specified version.
   */
  @SuppressWarnings("unused")
  private Map<Long, byte[]> filteredCopy(NavigableMap<Version, Value> columnMap, ReadPointer readPointer) {
    NavigableMap<Long, byte[]> ret = new TreeMap<Long, byte[]>();
    long lastDelete = -1;
    long undeleted = -1;
    for (Map.Entry<Version, Value> entry : columnMap.entrySet()) {
      Version curVersion = entry.getKey();
      if (!readPointer.isVisible(curVersion.stamp)) {
        continue;
      }
      if (curVersion.isUndeleteAll()) {
        undeleted = entry.getKey().stamp;
        continue;
      }
      if (curVersion.isDeleteAll()) {
        if (undeleted == curVersion.stamp) {
          continue;
        } else {
          break;
        }
      }
      if (curVersion.isDelete()) {
        lastDelete = entry.getKey().stamp;
        continue;
      }
      if (curVersion.stamp == lastDelete) {
        continue;
      }
      ret.put(curVersion.stamp, entry.getValue().getValue());
    }
    return ret.descendingMap();
  }

  /**
   * Returns the latest version of a column within the specified column map,
   * filtering out deleted values and everything with a version higher than the
   * specified version.
   */
  private ImmutablePair<Long, byte[]> latest(NavigableMap<Version, Value> columnMap) {
    if (columnMap == null || columnMap.isEmpty()) {
      return null;
    }
    long lastDelete = -1;
    long undeleted = -1;
    for (Map.Entry<Version, Value> entry : columnMap.entrySet()) {
      Version curVersion = entry.getKey();
      if (curVersion.isUndeleteAll()) {
        undeleted = entry.getKey().stamp;
        continue;
      }
      if (curVersion.isDeleteAll()) {
        if (undeleted == curVersion.stamp) {
          continue;
        } else {
          break;
        }
      }
      if (curVersion.isDelete()) {
        lastDelete = entry.getKey().stamp;
        continue;
      }
      if (curVersion.stamp == lastDelete) {
        continue;
      }
      return new ImmutablePair<Long, byte[]>(curVersion.stamp, entry.getValue().getValue());
    }
    return null;
  }

  /**
   * Returns the latest version of a column within the specified column map,
   * filtering out deleted values and everything with a version higher than the
   * specified version.
   */
  private ImmutablePair<Long, byte[]> filteredLatest(NavigableMap<Version, Value> columnMap, ReadPointer readPointer) {
    if (columnMap == null || columnMap.isEmpty()) {
      return null;
    }
    long lastDelete = -1;
    long undeleted = -1;
    for (Map.Entry<Version, Value> entry : columnMap.entrySet()) {
      Version curVersion = entry.getKey();
      if (!readPointer.isVisible(curVersion.stamp)) {
        continue;
      }
      if (curVersion.isUndeleteAll()) {
        undeleted = entry.getKey().stamp;
        continue;
      }
      if (curVersion.isDeleteAll()) {
        if (undeleted == curVersion.stamp) {
          continue;
        } else {
          break;
        }
      }
      if (curVersion.isDelete()) {
        lastDelete = entry.getKey().stamp;
        continue;
      }
      if (curVersion.stamp == lastDelete) {
        continue;
      }
      return new ImmutablePair<Long, byte[]>(curVersion.stamp, entry.getValue().getValue());
    }
    return null;
  }

  /**
   * Locks the specified row and returns the map of the columns of the row.
   */
  private NavigableMap<Column, NavigableMap<Version, Value>> getAndLockRow(RowLockTable.Row row) {

    // first obtain the row lock
    RowLockTable.RowLock rowLock;
    do {
      rowLock = this.locks.lock(row);
      // obtained a lock, but it may be invalid, loop until valid
      // this can happen because we evict locks from the table when a row is empty
    } while (!rowLock.isValid());

    // Now we have a definitive lock, see if the row exists
    NavigableMap<Column, NavigableMap<Version, Value>> rowMap = this.map.get(row);

    if (rowMap == null) { // create an empty row
      rowMap = new TreeMap<Column, NavigableMap<Version, Value>>();
      this.map.put(row, rowMap);
    }
    return rowMap;
  }

  /**
   * Locks the specified row and returns the map of the columns of the row.
   */
  private NavigableMap<Column, NavigableMap<Version, Value>> getAndLockExistingRow(RowLockTable.Row row) {

    NavigableMap<Column, NavigableMap<Version, Value>> rowMap;
    while (true) {
      // avoid adding new locks to the lock table for rows that do not exists: first check for existence
      rowMap = this.map.get(row);
      if (rowMap == null) {
        return null;
      }
      // Now that we found an existing row, grab a row lock, if it is invalid, we have to start over
      // (the row may just have been deleted)
      RowLockTable.RowLock lock = this.locks.lock(row);
      if (lock.isValid()) {
        return rowMap;
      }
    }
  }

  private NavigableMap<Version, Value> getColumn(NavigableMap<Column, NavigableMap<Version, Value>> rowMap,
                                                 byte[] column) {
    NavigableMap<Version, Value> columnMap = rowMap.get(new Column(column));
    if (columnMap == null) {
      columnMap = new TreeMap<Version, Value>();
      rowMap.put(new Column(column), columnMap);
    }
    return columnMap;
  }

  static class Column extends RowLockTable.Row {
    Column(final byte[] key) {
      super(key);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("column", Bytes.toString(super.getValue())).toString();
    }
  }

  static class Value extends RowLockTable.Row {
    final ValueType type;

    Value(final byte[] value) {
      super(value);
      this.type = ValueType.DATA;
    }

    Value(ValueType type) {
      super(null);
      this.type = type;
    }

    static Value delete() {
      return new Value(ValueType.DELETE);
    }

    enum ValueType {
      DATA, DELETE
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("type", this.type).toString();
    }
  }

  static class Version implements Comparable<Version> {
    final long stamp;
    final Type type;

    Version(long stamp) {
      this(stamp, Type.VALUE);
    }

    private Version(long stamp, Type type) {
      this.stamp = stamp;
      this.type = type;
    }

    public static Version delete(long stamp) {
      return new Version(stamp, Type.DELETE);
    }

    @Override
    public boolean equals(Object o) {
      return (o instanceof Version) && this.stamp == ((Version) o).stamp;
    }

    @Override
    public int hashCode() {
      return (int) this.stamp;
    }

    @Override
    public int compareTo(Version v) {
      if (this.stamp > v.stamp) {
        return -1;
      }
      if (this.stamp < v.stamp) {
        return 1;
      }
      return this.type.compareTo(v.type);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("stamp", this.stamp).add("type", this.type).toString();
    }

    public boolean isUndeleteAll() {
      return this.type == Type.UNDELETE_ALL;
    }

    public boolean isDeleteAll() {
      return this.type == Type.DELETE_ALL;
    }

    public boolean isDelete() {
      return this.type == Type.DELETE;
    }

    enum Type {
      UNDELETE_ALL, DELETE_ALL, DELETE, VALUE
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", Bytes.toString(this.name)).
      add("numrows", this.map.size()).add("numlocks", this.locks.size()).toString();
  }
}
