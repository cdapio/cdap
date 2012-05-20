/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.fabric.engine.table;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.common.utils.ImmutablePair;

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
public class MemoryVersionedTable implements VersionedTable {

  private final ConcurrentHashMap<Row,        // row to
      ConcurrentNavigableMap<Column,          // column to
        NavigableMap<Version,byte[]>>> map =  // version to value
    new ConcurrentHashMap<Row,
    ConcurrentNavigableMap<Column,
        NavigableMap<Version,byte[]>>>();

  private final ConcurrentHashMap<Row,RowLock> locks =
      new ConcurrentHashMap<Row,RowLock>();

  public MemoryVersionedTable() {}

  @Override
  public void put(byte [] row, byte [] column, byte [] value) {
    put(row, column, now(), value);
  }

  @Override
  public void put(byte [] row, byte [] column, long version, byte [] value) {
    NavigableMap<Column, NavigableMap<Version,byte[]>> rowMap =
        getAndLockRow(row);
    NavigableMap<Version,byte[]> columnMap = getColumn(rowMap, column);
    columnMap.put(new Version(version), value);
    unlockRow(row);
  }

  /**
   * Locks the specified row and returns the map of the columns of the row.
   * @param row
   * @return
   */
  private ImmutablePair<RowLock,
  NavigableMap<Column,NavigableMap<Version,byte[]>>> getAndLockRow(
      byte[] row) {
    NavigableMap<Column,NavigableMap<Version,byte[]>> rowMap =
        map.get(new Row(row));
    if (rowMap == null) {
      rowMap = new TreeMap<Column,NavigableMap<Version,byte[]>>();
      NavigableMap<Column,NavigableMap<Version,byte[]>> existingMap =
          map.putIfAbsent(new Row(row), rowMap);
      if (existingMap != null) return existingMap;
    }
    return rowMap;
  }

  /**
   * @param rowMap
   * @param column
   * @return
   */
  private NavigableMap<Version, byte[]> getColumn(
      ConcurrentNavigableMap<Column, NavigableMap<Version, byte[]>> rowMap,
      byte[] column) {
    NavigableMap<Version,byte[]> columnMap = rowMap.get(new Column(column));
    if (columnMap == null) {
      columnMap = new TreeMap<Version,byte[]>();
      NavigableMap<Version,byte[]> existingMap = rowMap.get(new Column(column));
      if (existingMap != null) return existingMap;
    }
    return columnMap;
  }

  @Override
  public void put(byte [] row, byte [][] columns, byte [][] values) {
    long [] versions = new long [row.length];
    long now = now();
    for(int i=0; i<versions.length; i++) versions[i] = now;
    put(row, columns, versions, values);
  }

  @Override
  public void put(byte [] row, byte [][] columns, long [] versions,
      byte [][] values) {

  }

  @Override
  public boolean delete(byte [] row) {
    return false;
  }

  @Override
  public boolean delete(byte [] row, byte [] column) {
    return false;
  }

  @Override
  public boolean delete(byte [] row, byte [] column, long version) {
    return false;
  }

  @Override
  public byte [][] getAllVersions(byte [] row, byte [] column) {
    return null;
  }

  @Override
  public byte [] get(byte [] row, byte [] column) {
    return get(row, column, now());
  }

  @Override
  public byte [] get(byte [] row, byte [] column, long maxVersion) {

    return null;
  }

  @Override
  public Map<byte[],byte[]> get(byte [] row, byte [][] columns,
      long maxVersion) {
    return null;
  }

  @Override
  public Map<byte[],byte[]> get(byte [] row, byte [][] columns) {
    return null;
  }

  private long now() {
    return System.currentTimeMillis();
  }

  class Row implements Comparable<Row> {
    final byte [] key;
    Row(byte [] key) {
      this.key = key;
    }
    @Override
    public boolean equals(Object o) {
      return Bytes.equals(key, ((Row)o).key);
    }
    @Override
    public int hashCode() {
      return Bytes.hashCode(key);
    }
    @Override
    public int compareTo(Row r) {
      return Bytes.compareTo(key, r.key);
    }
  }

  class Column extends Row {
    Column(byte[] key) {
      super(key);
    }
  }

  private final Random lockIdGenerator = new Random();

  class RowLock {
    final Row row;
    final long id;
    RowLock(Row row) {
      this.row = row;
      this.id = lockIdGenerator.nextLong();
    }
    @Override
    public boolean equals(Object o) {
      return id == ((RowLock)o).id &&
          Bytes.equals(row.key, ((RowLock)o).row.key);
    }
  }
  class Version implements Comparable<Version> {
    final long stamp;
    Version(long stamp) {
      this.stamp = stamp;
    }
    @Override
    public boolean equals(Object o) {
      return stamp == ((Version)o).stamp;
    }
    @Override
    public int compareTo(Version v) {
      if (stamp > v.stamp) return -1;
      if (stamp < v.stamp) return 1;
      return 0;
    }
  }
}
