package com.continuuity.internal.data.dataset.lib.table;

import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.internal.data.dataset.Dataset;

import javax.annotation.Nullable;

/**
 *
 */
public interface Table extends BatchReadable<byte[], Row>, BatchWritable<byte[], Put>, Dataset {
  /**
   * Reads values of all columns of the specified row.
   * NOTE: Depending on the implementation of this interface and use-case, calling this method may be much less
   *       efficient than calling the same method with columns as parameters because it may always require making a
   *       round trip to the persistent store.
   *
   * @param row row to read from
   * @return instance of {@link Row}; never {@code null}: returns empty {@link Row} if nothing to be read
   */
  Row get(byte[] row);

  /**
   * Reads the value of the specified column in the specified row.
   *
   *
   * @param row row to read from
   * @param column column to read value for
   * @return value of the column or {@code null} if value is absent
   */
  byte[] get(byte[] row, byte[] column);

  /**
   * Reads the values of the specified columns in the specified row.
   * @return instance of {@link Row}; never {@code null}: returns empty {@link Row} if nothing to be read
   */
  Row get(byte[] row, byte[][] columns);

  /**
   * Reads the values of all columns in the specified row that are
   * between the specified start (inclusive) and stop (exclusive) columns.
   *
   * @param startColumn beginning of range of columns, inclusive
   * @param stopColumn end of range of columns, exclusive
   * @param limit maximum number of columns to return
   * @return instance of {@link Row}; never {@code null}: returns empty {@link Row} if nothing to be read
   */
  Row get(byte[] row, byte[] startColumn, byte[] stopColumn, int limit);

  /**
   * Reads values of columns as defined by {@link Get} param.
   * @param get defines read selection
   * @return instance of {@link Row}; never {@code null}: returns empty {@link Row} if nothing to be read
   */
  Row get(Get get);

  /**
   * Writes the specified value for the specified column for the specified row.
   *
   * @param row row to write to
   * @param column column to write to
   * @param value to write
   */
  void put(byte[] row, byte[] column, byte[] value);

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
  void put(byte[] row, byte[][] columns, byte[][] values);

  /**
   * Writes values into a column of a row as defined by the {@link Put} parameter.
   * @param put defines values to write
   */
  void put(Put put);

  /**
   * Deletes all columns of the specified row.
   * NOTE: Depending on the implementation of this interface and use-case, calling this method may be much less
   *       efficient than calling same method with columns as parameters because it may require a round trip to
   *       the persistent store.
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
   *
   * NOTE: Depending on the implementation, this may work faster than calling {@link #delete(byte[], byte[])}
   *       multiple times (espcially in transactions that delete a lot of columns of the same row).
   *
   * @param row row to delete from
   * @param columns names of columns to delete
   */
  void delete(byte[] row, byte[][] columns);

  /**
   * Deletes columns of a row as defined by the {@link Delete} parameter.
   * @param delete defines what to delete
   */
  void delete(Delete delete);

  /**
   * Increments the specified column of a row by the specified amounts.
   *
   * @param row row which value to increment
   * @param column column to increment
   * @param amount amount to increment by
   * @return new value of the column
   * @throws NumberFormatException if stored value for the column is not in the serialized long value format
   */
  long increment(byte[] row, byte[] column, long amount);

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
   * @param startRow start row inclusive. {@code null} means start from first row of the table
   * @param stopRow stop row exclusive. {@code null} means scan all rows to the end of the table
   * @return instance of {@link Scanner}
   */
  Scanner scan(@Nullable byte[] startRow, @Nullable byte[] stopRow);
}
