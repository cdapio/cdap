/*
 * Copyright 2012-2014 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.data.dataset.table;

import com.continuuity.api.annotation.Beta;
import com.continuuity.api.annotation.Property;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.google.common.base.Supplier;

import java.util.List;
import javax.annotation.Nullable;

/**
 * This is the DataSet implementation of named tables -- other DataSets can be
 * defined by embedding instances of {@link Table} (and other DataSets).
 *
 * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.dataset.table.Table}
 */
@Deprecated
public class Table extends DataSet implements
  BatchReadable<byte[], Row>, BatchWritable<byte[], Put> {

  @Property
  private ConflictDetection conflictLevel;

  @Property
  private int ttl;

  // The actual table to delegate operations to. The value is injected by the runtime system.
  private Supplier<Table> delegate = new Supplier<Table>() {
    @Override
    public Table get() {
      throw new IllegalStateException("Delegate is not set. Are you calling runtime methods at configuration time?");
    }
  };

  /**
   * Constructor by name.
   * @param name the name of the table
   */
  public Table(String name) {
    this(name, ConflictDetection.ROW);
  }

  /**
   * Constructor by name.
   * @param name the name of the table
   * @param ttl time to live for data written into a table, in ms. Negative means unlimited
   */
  public Table(String name, int ttl) {
    this(name, ConflictDetection.ROW, ttl);
  }

  /**
   * Constructor by name.
   * @param name the name of the table
   * @param level level on which to detect conflicts in changes made by different transactions
   */
  public Table(String name, ConflictDetection level) {
    this(name, level, -1);
  }

  /**
   * Constructor by name.
   * @param name the name of the table
   * @param level level on which to detect conflicts in changes made by different transactions
   * @param ttl time to live for data written into a table, in ms. Negative means unlimited
   */
  public Table(String name, ConflictDetection level, int ttl) {
    super(name);
    this.conflictLevel = level;
    this.ttl = ttl;
  }

  /**
   * Defines level on which to resolve conflicts of the changes made in different transactions.
   */
  public static enum ConflictDetection {
    ROW,
    COLUMN,
    @Beta
    NONE
  }

  /**
   * Helper to return the name of the physical table. Currently the same as
   * the name of the dataset (the name of the table).
   * @return the name of the underlying table in the data fabric
   */
  protected String tableName() {
    return this.getName();
  }

  /**
   * @return conflict detection level
   */
  public ConflictDetection getConflictLevel() {
    return conflictLevel;
  }

  /**
   * @return time to live setting
   */
  public int getTTL() {
    return ttl;
  }

  // Basic data operations

  /**
   * Reads values of all columns of the specified row.
   * NOTE: Depending on the implementation of this interface and use-case, calling this method may be much less
   *       efficient than calling the same method with columns as parameters because it may always require making a
   *       round trip to the persistent store.
   *
   * @param row row to read from
   * @return instance of {@link Row}; never {@code null}: returns empty {@link Row} if nothing to be read
   */
  public Row get(byte[] row) {
    return delegate.get().get(row);
  }

  /**
   * Reads the value of the specified column in the specified row.
   *
   *
   * @param row row to read from
   * @param column column to read value for
   * @return value of the column or {@code null} if value is absent
   */
  public byte[] get(byte[] row, byte[] column) {
    return delegate.get().get(row, column);
  }

  /**
   * Reads the values of the specified columns in the specified row.
   * @return instance of {@link Row}; never {@code null}: returns empty {@link Row} if nothing to be read
   */
  public Row get(byte[] row, byte[][] columns) {
    return delegate.get().get(row, columns);
  }

  /**
   * Reads the values of all columns in the specified row that are
   * between the specified start (inclusive) and stop (exclusive) columns.
   *
   * @param startColumn beginning of range of columns, inclusive
   * @param stopColumn end of range of columns, exclusive
   * @param limit maximum number of columns to return
   * @return instance of {@link Row}; never {@code null}: returns empty {@link Row} if nothing to be read
   */
  public Row get(byte[] row, byte[] startColumn, byte[] stopColumn, int limit) {
    return delegate.get().get(row, startColumn, stopColumn, limit);
  }

  /**
   * Reads values of columns as defined by {@link Get} param.
   * @param get defines read selection
   * @return instance of {@link Row}; never {@code null}: returns empty {@link Row} if nothing to be read
   */
  public Row get(Get get) {
    return delegate.get().get(get);
  }

  /**
   * Writes the specified value for the specified column for the specified row.
   *
   * @param row row to write to
   * @param column column to write to
   * @param value to write
   */
  public void put(byte[] row, byte[] column, byte[] value) {
    delegate.get().put(row, column, value);
  }

  /**
   * Writes the specified values for the specified columns of the specified row.
   *
   * NOTE: Depending on the implementation, this may work faster than calling {@link #put(byte[], byte[], byte[])}
   *       multiple times (espcially in transactions that change a lot of columns of one row).
   *
   * @param row row to write to
   * @param columns columns to write to
   * @param values array of values to write (same order as values)
   */
  public void put(byte[] row, byte[][] columns, byte[][] values) {
    delegate.get().put(row, columns, values);
  }

  /**
   * Writes values into a column of a row as defined by the {@link Put} parameter.
   * @param put defines values to write
   */
  public void put(Put put) {
     delegate.get().put(put);
  }

  /**
   * Deletes all columns of the specified row.
   * NOTE: Depending on the implementation of this interface and use-case, calling this method may be much less
   *       efficient than calling same method with columns as parameters because it may require a round trip to
   *       the persistent store.
   *
   * @param row row to delete
   */
  public void delete(byte[] row) {
    delegate.get().delete(row);
  }

  /**
   * Deletes specified column of the specified row.
   *
   * @param row row to delete from
   * @param column column name to delete
   */
  public void delete(byte[] row, byte[] column) {
    delegate.get().delete(row, column);
  }

  /**
   * Deletes specified columns of the specified row.
   *
   * NOTE: Depending on the implementation, this may work faster than calling {@link #delete(byte[], byte[])}
   *       multiple times (espcially in transactions that delete a lot of columns of the same row).
   *
   * @param row row to delete from
   * @param columns names of columns to delete
   */
  public void delete(byte[] row, byte[][] columns) {
    delegate.get().delete(row, columns);
  }

  /**
   * Deletes columns of a row as defined by the {@link Delete} parameter.
   * @param delete defines what to delete
   */
  public void delete(Delete delete) {
    delegate.get().delete(delete);
  }

  /**
   * Increments the specified column of a row by the specified amounts.
   *
   * @param row row which value to increment
   * @param column column to increment
   * @param amount amount to increment by
   * @return new value of the column
   * @throws NumberFormatException if stored value for the column is not in the serialized long value format
   */
  public long increment(byte[] row, byte[] column, long amount) {
    return delegate.get().increment(row, column, amount);
  }

  /**
   * Increments the specified columns of a row by the specified amounts.
   *
   * NOTE: depending on the implementation this may work faster than calling {@link #increment(byte[], byte[], long)}
   *       multiple times (esp. in transaction that increments a lot of columns of same rows)
   *
   * @param row row which values to increment
   * @param columns columns to increment
   * @param amounts amounts to increment columns by (same order as columns)
   * @return {@link Row} with a subset of changed columns
   * @throws NumberFormatException if stored value for the column is not in the serialized long value format
   */
  public Row increment(byte[] row, byte[][] columns, long[] amounts) {
    return delegate.get().increment(row, columns, amounts);
  }

  /**
   * Increments the specified columns of a row by the specified amounts defined by the {@link Increment} parameter.
   *
   * @param increment defines changes
   * @return {@link Row} with a subset of changed columns
   * @throws NumberFormatException if stored value for the column is not in the serialized long value format
   */
  public Row increment(Increment increment) {
    return delegate.get().increment(increment);
  }

  /**
   * Compares-and-swaps the value of the specified row and column
   * by looking for the specified expected value, and if found, replacing with
   * the new specified value.
   * NOTE: Has no affect on data when fails (stored value is different from the expected value). Returns {@code false}
   *       in this case.
   *
   * @param row row to modify
   * @param column column to change
   * @param expectedValue expected value before change
   * @param newValue value to set
   * @return true if compare and swap succeeded, false otherwise (stored value is different from expected)
   */
  public boolean compareAndSwap(byte[] row, byte[] column,
                                byte[] expectedValue, byte[] newValue) {
    return delegate.get().compareAndSwap(row, column, expectedValue, newValue);
  }

  /**
   * Scans table.
   *
   * @param startRow start row inclusive. {@code null} means start from first row of the table
   * @param stopRow stop row exclusive. {@code null} means scan all rows to the end of the table
   * @return instance of {@link Scanner}
   */
  public Scanner scan(@Nullable byte[] startRow, @Nullable byte[] stopRow) {
    return delegate.get().scan(startRow, stopRow);
  }

  @Override
  public List<Split> getSplits() {
    return getSplits(-1, null, null);
  }

  /**
   * Returns splits for a range of keys in the table.
   *
   * @param numSplits Desired number of splits. If greater than zero, at most this many splits will be returned.
   *                  If less or equal to zero, any number of splits can be returned.
   * @param start If non-null, the returned splits will only cover keys that are greater or equal.
   * @param stop If non-null, the returned splits will only cover keys that are less.
   * @return list of {@link Split}
   */
  @Beta
  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) {
    return delegate.get().getSplits(numSplits, start, stop);
  }

  @Override
  public SplitReader<byte[], Row> createSplitReader(Split split) {
    return delegate.get().createSplitReader(split);
  }

  @Override
  public void write(byte[] key, Put put) {
    delegate.get().write(key, put);
  }
}
