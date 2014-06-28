/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.dataset.table;

import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.dataset.Dataset;

import java.util.List;
import javax.annotation.Nullable;

/**
 * An ordered, named table.
 */
public interface Table extends BatchReadable<byte[], Row>, BatchWritable<byte[], Put>, Dataset {
  /**
   * Reads values of all columns of the specified row.
   * <p>
   * NOTE: Depending on the implementation of this interface and use-case, calling this method can be less
   * efficient than calling the same method with columns as parameters because it can require making a
   * round trip to the persistent store.
   *
   * @param row row to read from
   * @return instance of {@link Row}: never {@code null}; returns an empty Row if nothing read
   */
  Row get(byte[] row);

  /**
   * Reads the value of the specified column of the specified row.
   *
   * @param row row to read from
   * @param column column to read value for
   * @return value of the column or {@code null} if the value is absent
   */
  byte[] get(byte[] row, byte[] column);

  /**
   * Reads the values of the specified columns of the specified row.
   *
   * @return instance of {@link Row}: never {@code null}; returns an empty Row if nothing read
   */
  Row get(byte[] row, byte[][] columns);

  /**
   * Reads the values of all columns in the specified row that are
   * between the specified start (inclusive) and stop (exclusive) columns.
   *
   * @param startColumn beginning of range of columns, inclusive
   * @param stopColumn end of range of columns, exclusive
   * @param limit maximum number of columns to return
   * @return instance of {@link Row}; never {@code null}; returns an empty Row if nothing read
   */
  Row get(byte[] row, byte[] startColumn, byte[] stopColumn, int limit);

  /**
   * Reads values of columns as defined by {@link Get} parameter.
   *
   * @param get defines read selection
   * @return instance of {@link Row}: never {@code null}; returns an empty Row if nothing read
   */
  Row get(Get get);

  /**
   * Writes the specified value for the specified column of the specified row.
   *
   * @param row row to write to
   * @param column column to write to
   * @param value to write
   */
  void put(byte[] row, byte[] column, byte[] value);

  /**
   * Writes the specified values for the specified columns of the specified row.
   * <p>
   * NOTE: Depending on the implementation, this can work faster than calling {@link #put(byte[], byte[], byte[])}
   * multiple times (espcially in transactions that alter many columns of one row).
   *
   * @param row row to write to
   * @param columns columns to write to
   * @param values array of values to write (same order as values)
   */
  void put(byte[] row, byte[][] columns, byte[][] values);

  /**
   * Writes values into a column of a row as defined by the {@link Put} parameter.
   *
   * @param put defines values to write
   */
  void put(Put put);

  /**
   * Deletes all columns of the specified row.
   * <p>
   * NOTE: Depending on the implementation of this interface and use-case, calling this method can be less
   * efficient than calling the same method with columns as parameters because it can require a round trip to
   * the persistent store.
   *
   * @param row row to delete
   */
  void delete(byte[] row);

  /**
   * Deletes specified column of the specified row.
   *
   * @param row row to delete from
   * @param column column name to delete
   */
  void delete(byte[] row, byte[] column);

  /**
   * Deletes specified columns of the specified row.
   * <p>
   * NOTE: Depending on the implementation, this can work faster than calling {@link #delete(byte[], byte[])}
   * multiple times (especially in transactions that delete many columns of the same rows).
   *
   * @param row row to delete from
   * @param columns names of columns to delete
   */
  void delete(byte[] row, byte[][] columns);

  /**
   * Deletes columns of a row as defined by the {@link Delete} parameter.
   *
   * @param delete defines what to delete
   */
  void delete(Delete delete);

  /**
   * Increments the specified column of the row by the specified amounts.
   *
   * @param row row which value to increment
   * @param column column to increment
   * @param amount amount to increment by
   * @return new value of the column
   * @throws NumberFormatException if stored value for the column is not in the serialized long value format
   */
  long increment(byte[] row, byte[] column, long amount);

  /**
   * Increments the specified columns of the row by the specified amounts.
   * <p>
   * NOTE: Depending on the implementation, this can work faster than calling {@link #increment(byte[], byte[], long)}
   * multiple times (especially in a transaction that increments multiple columns of the same rows)
   *
   * @param row row whose values to increment
   * @param columns columns to increment
   * @param amounts amounts to increment columns by (in the same order as the columns)
   * @return {@link Row} with a subset of changed columns
   * @throws NumberFormatException if stored value for the column is not in the serialized long value format
   */
  Row increment(byte[] row, byte[][] columns, long[] amounts);

  /**
   * Increments the specified columns of a row by the specified amounts defined by the {@link Increment} parameter.
   *
   * @param increment defines changes
   * @return {@link Row} with a subset of changed columns
   * @throws NumberFormatException if stored value for the column is not in the serialized long value format
   */
  Row increment(Increment increment);

  /**
   * Scans table.
   *
   * @param startRow start row inclusive; {@code null} means start from first row of the table
   * @param stopRow stop row exclusive; {@code null} means scan all rows to the end of the table
   * @return instance of {@link Scanner}
   */
  Scanner scan(@Nullable byte[] startRow, @Nullable byte[] stopRow);

  /**
   * Returns splits for a range of keys in the table.
   * 
   * @param numSplits Desired number of splits. If greater than zero, at most this many splits will be returned.
   *                  If less than or equal to zero, any number of splits can be returned.
   * @param start if non-null, the returned splits will only cover keys that are greater or equal
   * @param stop if non-null, the returned splits will only cover keys that are less
   * @return list of {@link Split}
   */
  List<Split> getSplits(int numSplits, byte[] start, byte[] stop);


  /**
   * Compares-and-swaps (atomically) the value of the specified row and column by looking for
   * an expected value and, if found, replacing it with the new value.
   *
   * @param key row to modify
   * @param keyColumn column to modify
   * @param oldValue expected value before change
   * @param newValue value to set
   * @return true if compare and swap succeeded, false otherwise (stored value is different from expected)
   */
  boolean compareAndSwap(byte[] key, byte[] keyColumn, byte[] oldValue, byte[] newValue) throws Exception;
}
