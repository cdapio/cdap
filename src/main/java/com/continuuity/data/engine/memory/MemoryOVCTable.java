/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.engine.memory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.ReadPointer;
import com.continuuity.data.table.Scanner;
import com.google.common.base.Objects;

/**
 * An in-memory implementation of a column-oriented table similar to HBase.
 * 
 * A row has one or more columns, and a column has one or more versions.
 * 
 * Columns are sorted in ascending binary order, versions of a column are sorted
 * in descending timestamp order.
 * 
 * This version of MemoryTable is currently NOT sorted by row.
 */
public class MemoryOVCTable implements OrderedVersionedColumnarTable {

  private final byte[] name;

  private final ConcurrentNavigableMap<Row, // row to
  NavigableMap<Column, // column to
  NavigableMap<Version, Value>>> map = // version to value
  new ConcurrentSkipListMap<Row, NavigableMap<Column, NavigableMap<Version, Value>>>();

  private final ConcurrentHashMap<Row, RowLock> locks = new ConcurrentHashMap<Row, RowLock>();

  public MemoryOVCTable(final byte [] tableName) {
    this.name = tableName;
  }

  @Override
  public void put(byte[] row, byte[] column, long version, byte[] value) {
    put(row, new byte[][] { column }, version, new byte[][] { value });
  }

  @Override
  public void put(byte[] row, byte[][] columns, long version, byte[][] values) {
    assert (columns.length == values.length);
    Row r = new Row(row);
    ImmutablePair<RowLock, NavigableMap<Column, NavigableMap<Version, Value>>> p = getAndLockRow(r);
    for (int i = 0; i < columns.length; i++) {
      NavigableMap<Version, Value> columnMap = getColumn(p.getSecond(),
          columns[i]);
      columnMap.put(new Version(version), new Value(values[i]));
    }
    unlockRow(r);
  }

  @Override
  public void delete(byte[] row, byte[] column, long version) {
    Row r = new Row(row);
    ImmutablePair<RowLock, NavigableMap<Column, NavigableMap<Version, Value>>> p = getAndLockRow(r);
    NavigableMap<Version, Value> columnMap = getColumn(p.getSecond(), column);
    columnMap.put(Version.delete(version), Value.delete());
    unlockRow(r);
  }

  @Override
  public void deleteAll(byte[] row, byte[] column, long version) {
    Row r = new Row(row);
    ImmutablePair<RowLock, NavigableMap<Column, NavigableMap<Version, Value>>> p = getAndLockRow(r);
    NavigableMap<Version, Value> columnMap = getColumn(p.getSecond(), column);
    columnMap.put(Version.deleteAll(version), Value.delete());
    unlockRow(r);
  }

  @Override
  public void undeleteAll(byte[] row, byte[] column, long version) {
    Row r = new Row(row);
    ImmutablePair<RowLock, NavigableMap<Column, NavigableMap<Version, Value>>> p = getAndLockRow(r);
    NavigableMap<Version, Value> columnMap = getColumn(p.getSecond(), column);
    columnMap.put(Version.undeleteAll(version), Value.delete());
    unlockRow(r);
  }

  @Override
  public Map<byte[], byte[]> get(byte[] row, ReadPointer pointer) {
    Row r = new Row(row);
    ImmutablePair<RowLock, NavigableMap<Column, NavigableMap<Version, Value>>> p = getAndLockRow(r);
    Map<byte[], byte[]> ret = new TreeMap<byte[], byte[]>(
        Bytes.BYTES_COMPARATOR);
    for (Map.Entry<Column, NavigableMap<Version, Value>> entry : p.getSecond()
        .entrySet()) {
      ImmutablePair<Long, byte[]> latest = filteredLatest(entry.getValue(),
          pointer);
      if (latest == null) continue;
      ret.put(entry.getKey().value, latest.getSecond());
    }
    unlockRow(r);
    return ret;
  }

  @Override
  public byte[] get(byte[] row, byte[] column, ReadPointer readPointer) {
    Row r = new Row(row);
    ImmutablePair<RowLock, NavigableMap<Column, NavigableMap<Version, Value>>> p = getAndLockRow(r);
    NavigableMap<Version, Value> columnMap = getColumn(p.getSecond(), column);
    ImmutablePair<Long, byte[]> latest = filteredLatest(columnMap, readPointer);
    unlockRow(r);
    if (latest == null) return null;
    return latest.getSecond();
  }

  @Override
  public ImmutablePair<byte[], Long> getWithVersion(byte[] row, byte[] column,
      ReadPointer readPointer) {
    Row r = new Row(row);
    ImmutablePair<RowLock, NavigableMap<Column, NavigableMap<Version, Value>>> p = getAndLockRow(r);
    NavigableMap<Version, Value> columnMap = getColumn(p.getSecond(), column);
    ImmutablePair<Long, byte[]> latest = filteredLatest(columnMap, readPointer);
    unlockRow(r);
    if (latest == null) return null;
    return new ImmutablePair<byte[],Long>(latest.getSecond(), latest.getFirst());
  }

  @Override
  public Map<byte[], byte[]> get(byte[] row, byte[][] columns,
      ReadPointer readPointer) {
    Row r = new Row(row);
    ImmutablePair<RowLock, NavigableMap<Column, NavigableMap<Version, Value>>> p = getAndLockRow(r);
    Map<byte[], byte[]> ret = new TreeMap<byte[], byte[]>(
        Bytes.BYTES_COMPARATOR);
    for (int i = 0; i < columns.length; i++) {
      byte[] column = columns[i];
      NavigableMap<Version, Value> columnMap = getColumn(p.getSecond(), column);
      ret.put(column, filteredLatest(columnMap, readPointer).getSecond());
    }
    unlockRow(r);
    return ret;
  }

  @Override
  public Map<byte[], byte[]> get(byte[] row, byte[] startColumn,
      byte[] stopColumn, ReadPointer readPointer) {
    Row r = new Row(row);
    ImmutablePair<RowLock, NavigableMap<Column, NavigableMap<Version, Value>>> p = getAndLockRow(r);
    Map<byte[], byte[]> ret = new TreeMap<byte[], byte[]>(
        Bytes.BYTES_COMPARATOR);
    SortedMap<Column, NavigableMap<Version, Value>> sub = p.getSecond().subMap(
        new Column(startColumn), new Column(stopColumn));
    for (Map.Entry<Column, NavigableMap<Version, Value>> entry : sub.entrySet()) {
      NavigableMap<Version, Value> columnMap = entry.getValue();
      ret.put(entry.getKey().value, filteredLatest(columnMap, readPointer)
          .getSecond());
    }
    unlockRow(r);
    return ret;
  }

  @Override
  public List<byte[]> getKeys(int limit, int offset, ReadPointer readPointer) {
    List<byte[]> keys = new ArrayList<byte[]>(limit > 1024 ? 1024 : limit);
    int returned = 0;
    int skipped = 0;

    for (Map.Entry<Row, NavigableMap<Column, NavigableMap<Version, Value>>> entry :
      this.map.entrySet()) {
      // Determine if row is visible
      if (hasAnyVisibleColumns(entry.getValue(), readPointer)) {
        if (skipped < offset) {
          skipped++;
        } else if (returned < limit) {
          returned++;
          keys.add(entry.getKey().value);
        }
        if (returned == limit) return keys;
      }
    }
    return keys;
  }
    
  private boolean hasAnyVisibleColumns(
      NavigableMap<Column, NavigableMap<Version, Value>> columns,
      ReadPointer readPointer) {
    for (Map.Entry<Column, NavigableMap<Version,Value>> entry :
      columns.entrySet()) {
      if (filteredLatest(entry.getValue(), readPointer) != null) {
        return true;
      }
    }
    return false;
  }
    
  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow,
      ReadPointer readPointer) {
    return new MemoryScanner(this.map.subMap(
        new Row(startRow), new Row(stopRow)).entrySet().iterator(),
        readPointer);
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow,
      byte[][] columns, ReadPointer readPointer) {
    return new MemoryScanner(this.map.subMap(
        new Row(startRow), new Row(stopRow)).entrySet().iterator(),
        columns, readPointer);
  }

  @Override
  public Scanner scan(ReadPointer readPointer) {
    return new MemoryScanner(this.map.entrySet().iterator(), readPointer);
  }

  public class MemoryScanner implements Scanner {

    private final Iterator<Entry<Row,
    NavigableMap<Column, NavigableMap<Version, Value>>>> rows;

    private final ReadPointer readPointer;

    private final Set<byte[]> columnSet;

    public MemoryScanner(Iterator<Entry<Row, NavigableMap<Column,
        NavigableMap<Version, Value>>>> rows,
        ReadPointer readPointer) {
      this(rows, null, readPointer);
    }

    public MemoryScanner(Iterator<Entry<Row, NavigableMap<Column,
        NavigableMap<Version, Value>>>> rows,
        byte [][] columns, ReadPointer readPointer) {
      this.rows = rows;
      this.readPointer = readPointer;
      this.columnSet = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
      if (columns != null) for (byte [] column : columns) this.columnSet.add(column);
    }

    @Override
    public ImmutablePair<byte[], Map<byte[], byte[]>> next() {
      if (!this.rows.hasNext()) return null;
      Entry<Row,NavigableMap<Column, NavigableMap<Version, Value>>> rowEntry =
          this.rows.next();
      Map<byte[],byte[]> columns = new TreeMap<byte[],byte[]>(
          Bytes.BYTES_COMPARATOR);
      for (Map.Entry<Column, NavigableMap<Version,Value>> colEntry :
        rowEntry.getValue().entrySet()) {
        if (!this.columnSet.contains(colEntry.getKey().value)) continue;
        byte [] value =
            filteredLatest(colEntry.getValue(), this.readPointer).getSecond();
        if (value != null) columns.put(colEntry.getKey().value, value);
      }
      return new ImmutablePair<byte[], Map<byte[],byte[]>>(
          rowEntry.getKey().value, columns);
    }

    @Override
    public void close() { /* no op */ }

  }

  @Override
  public long increment(byte[] row, byte[] column, long amount,
      ReadPointer readPointer, long writeVersion) {
    Row r = new Row(row);
    ImmutablePair<RowLock, NavigableMap<Column, NavigableMap<Version, Value>>> p = getAndLockRow(r);
    NavigableMap<Version, Value> columnMap = getColumn(p.getSecond(), column);
    long existingAmount = 0L;
    ImmutablePair<Long, byte[]> latest = filteredLatest(columnMap, readPointer);
    if (latest != null) existingAmount = Bytes.toLong(latest.getSecond());
    long newAmount = existingAmount + amount;
    columnMap.put(new Version(writeVersion),
        new Value(Bytes.toBytes(newAmount)));
    unlockRow(r);
    return newAmount;
  }

  @Override
  public boolean compareAndSwap(byte[] row, byte[] column,
      byte[] expectedValue, byte[] newValue, ReadPointer readPointer,
      long writeVersion) {
    Row r = new Row(row);
    ImmutablePair<RowLock, NavigableMap<Column, NavigableMap<Version, Value>>> p = getAndLockRow(r);
    try {
      NavigableMap<Version, Value> columnMap = getColumn(p.getSecond(), column);
      ImmutablePair<Long, byte[]> latest = filteredLatest(columnMap,
          readPointer);
      if (latest == null && expectedValue != null) return false;
      if (latest != null && expectedValue == null) return false;
      if (latest != null && expectedValue != null
          && !Bytes.equals(latest.getSecond(), expectedValue)) {
        return false;
      }
      if (newValue == null || newValue.length == 0) {
        p.getSecond().remove(new Column(column));
      } else {
        columnMap.put(new Version(writeVersion), new Value(newValue));
      }
      return true;
    } finally {
      unlockRow(r);
    }
  }

  // Private helpers

  /**
   * Makes a copy of all visible versions of columns within the specified column
   * map, filtering out deleted values and everything with a version higher than
   * the specified version.
   * 
   * @param columnMap
   * @param maxVersion
   * @return
   */
  @SuppressWarnings("unused")
  private Map<Long, byte[]> filteredCopy(
      NavigableMap<Version, Value> columnMap, ReadPointer readPointer) {
    NavigableMap<Long, byte[]> ret = new TreeMap<Long, byte[]>();
    long lastDelete = -1;
    long undeleted = -1;
    for (Map.Entry<Version, Value> entry : columnMap.entrySet()) {
      Version curVersion = entry.getKey();
      if (!readPointer.isVisible(curVersion.stamp)) continue;
      if (curVersion.isUndeleteAll()) {
        undeleted = entry.getKey().stamp;
        continue;
      }
      if (curVersion.isDeleteAll()) {
        if (undeleted == curVersion.stamp) continue;
        else break;
      }
      if (curVersion.isDelete()) {
        lastDelete = entry.getKey().stamp;
        continue;
      }
      if (curVersion.stamp == lastDelete) continue;
      ret.put(curVersion.stamp, entry.getValue().value);
    }
    return ret.descendingMap();
  }

  /**
   * Returns the latest version of a column within the specified column map,
   * filtering out deleted values and everything with a version higher than the
   * specified version.
   * 
   * @param columnMap
   * @param maxVersion
   * @return
   */
  private ImmutablePair<Long, byte[]> filteredLatest(
      NavigableMap<Version, Value> columnMap, ReadPointer readPointer) {
    if (columnMap == null || columnMap.isEmpty()) return null;
    long lastDelete = -1;
    long undeleted = -1;
    for (Map.Entry<Version, Value> entry : columnMap.entrySet()) {
      Version curVersion = entry.getKey();
      if (!readPointer.isVisible(curVersion.stamp)) continue;
      if (curVersion.isUndeleteAll()){
        undeleted = entry.getKey().stamp;
        continue;
      }
      if (curVersion.isDeleteAll()) {
        if (undeleted == curVersion.stamp) continue;
        else break;
      }
      if (curVersion.isDelete()) {
        lastDelete = entry.getKey().stamp;
        continue;
      }
      if (curVersion.stamp == lastDelete) continue;
      return new ImmutablePair<Long, byte[]>(curVersion.stamp,
          entry.getValue().value);
    }
    return null;
  }

  /**
   * Locks the specified row and returns the map of the columns of the row.
   * 
   * @param row
   * @return
   */
  private ImmutablePair<RowLock, NavigableMap<Column,
          NavigableMap<Version, Value>>> getAndLockRow(Row row) {
    // Ensure row entry exists
    NavigableMap<Column, NavigableMap<Version, Value>> rowMap = this.map
        .get(row);
    if (rowMap == null) {
      rowMap = new TreeMap<Column, NavigableMap<Version, Value>>();
      NavigableMap<Column, NavigableMap<Version, Value>> existingMap = this.map
          .putIfAbsent(row, rowMap);
      if (existingMap != null) rowMap = existingMap;
    }
    // Now we have the entry, grab a row lock
    RowLock rowLock = lockRow(row);
    return new ImmutablePair<RowLock, NavigableMap<Column, NavigableMap<Version, Value>>>(
        rowLock, rowMap);
  }

  private RowLock lockRow(Row row) {
    RowLock lock = this.locks.get(row);
    if (lock == null) {
      lock = new RowLock(row);
      RowLock existing = this.locks.putIfAbsent(row, lock);
      if (existing != null) lock = existing;
    }
    lock.lock();
    return lock;
  }

  private void unlockRow(Row row) {
    RowLock lock = this.locks.get(row);
    if (lock == null) {
      throw new RuntimeException("Attempted to unlock invalid row lock");
    }
    lock.unlock();
  }

  /**
   * @param rowMap
   * @param column
   * @return
   */
  private NavigableMap<Version, Value> getColumn(
      NavigableMap<Column, NavigableMap<Version, Value>> rowMap, byte[] column) {
    NavigableMap<Version, Value> columnMap = rowMap.get(new Column(column));
    if (columnMap == null) {
      columnMap = new TreeMap<Version, Value>();
      rowMap.put(new Column(column), columnMap);
    }
    return columnMap;
  }

  static class Row implements Comparable<Row> {
    final byte[] value;

    Row(final byte[] value) {
      this.value = value;
    }

    @Override
    public boolean equals(Object o) {
      return Bytes.equals(this.value, ((Row) o).value);
    }

    @Override
    public int hashCode() {
      return Bytes.hashCode(this.value);
    }

    @Override
    public int compareTo(Row r) {
      return Bytes.compareTo(this.value, r.value);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("rowkey", Bytes.toString(this.value)).toString();
    }
  }

  static class Column extends Row {
    Column(final byte[] key) {
      super(key);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("column", Bytes.toString(super.value)).toString();
    }
  }

  static class Value extends Row {
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

  private static final Random lockIdGenerator = new Random();

  static class RowLock {
    final Row row;
    final long id;
    final AtomicBoolean locked;

    RowLock(Row row) {
      this.row = row;
      this.id = MemoryOVCTable.lockIdGenerator.nextLong();
      this.locked = new AtomicBoolean(false);
    }

    public boolean unlock() {
      synchronized (this.locked) {
        boolean ret = this.locked.compareAndSet(true, false);
        this.locked.notifyAll();
        return ret;
      }
    }

    public void lock() {
      synchronized (this.locked) {
        while (this.locked.get()) {
          try {
            this.locked.wait();
          } catch (InterruptedException e) {
            System.err.println("RowLock.lock() interrupted");
          }
        }
        if (this.locked.compareAndSet(false, true)) {
          return;
        }
      }
    }

    @Override
    public boolean equals(Object o) {
      return this.id == ((RowLock) o).id
          && Bytes.equals(this.row.value, ((RowLock) o).row.value);
    }

    @Override
    public int hashCode() {
      return Bytes.hashCode(this.row.value);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("row", this.row)
          .add("id", this.id).add("locked", this.locked).toString();
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

    public static Version deleteAll(long stamp) {
      return new Version(stamp, Type.DELETE_ALL);
    }

    public static Version undeleteAll(long stamp) {
      return new Version(stamp, Type.UNDELETE_ALL);
    }

    @Override
    public boolean equals(Object o) {
      return this.stamp == ((Version) o).stamp;
    }

    @Override
    public int hashCode() {
      return (int)this.stamp;
    }

    @Override
    public int compareTo(Version v) {
      if (this.stamp > v.stamp) return -1;
      if (this.stamp < v.stamp) return 1;
      return this.type.compareTo(v.type);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("stamp", this.stamp)
          .add("type", this.type)
          .toString();
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
      UNDELETE_ALL, DELETE_ALL, DELETE, VALUE;
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("name", Bytes.toString(this.name))
        .add("numrows", this.map.size())
        .add("numlocks", this.locks.size())
        .toString();
  }
}
