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

package co.cask.cdap.api.dataset.table;

import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.dataset.Dataset;

import java.util.List;
import java.util.Map;

/**
 * Provides generic table dataset interface.
 * @deprecated since 2.8.0.  Use {@link Table} instead.
 */
@Deprecated
public interface OrderedTable extends Dataset {

  /**
   * @deprecated since 2.8.0. Use {@link Table#PROPERTY_TTL} instead.
   */
  @Deprecated
  String PROPERTY_TTL = Table.PROPERTY_TTL;

  /**
   * @deprecated since 2.8.0. Use {@link Table#PROPERTY_READLESS_INCREMENT} instead.
   */
  @Deprecated
  String PROPERTY_READLESS_INCREMENT = Table.PROPERTY_READLESS_INCREMENT;

  /**
   * Reads the values of the specified columns in the specified row.
   * <p>
   * NOTE: objects that are passed in parameters can be re-used by underlying implementation and present
   *       in returned data structures from this method.
   * @return map of columns to values, never null
   */
  Map<byte[], byte[]> get(byte[] row, byte[][] columns) throws Exception;

  /**
   * Reads values of all columns in the specified row
   * NOTE: depending on the implementation of this interface and use-case, calling this method may be much less
   *       efficient than calling same method with columns as parameters because it may always require round trip to
   *       persistent store
   * <p>
   * NOTE: objects that are passed in parameters can be re-used by underlying implementation and present
   *       in returned data structures from this method.
   *
   * @param row row to read from
   * @return map of columns to values, never null
   */
  Map<byte [], byte []> get(byte[] row) throws Exception;

  /**
   * Reads the value of the specified column in the specified row.
   *
   * @param row row to read from
   * @param column column to read value for
   * @return value of the column or {@code null} if value is absent
   */
  byte[] get(byte[] row, byte[] column) throws Exception;

  /**
   * Reads the values of all columns in the specified row that are
   * between the specified start (inclusive) and stop (exclusive) columns.
   * <p>
   * NOTE: objects that are passed in parameters can be re-used by underlying implementation and present
   *       in returned data structures from this method.
   *
   * @param startColumn beginning of range of columns, inclusive
   * @param stopColumn end of range of columns, exclusive
   * @param limit maximum number of columns to return
   * @return map of columns to values, never null
   */
  Map<byte[], byte[]> get(byte[] row, byte[] startColumn, byte[] stopColumn, int limit) throws Exception;

  /**
   * Writes the specified values for the specified columns for the specified row.
   *
   * NOTE: depending on the implementation this may work faster than calling {@link #put(byte[], byte[], byte[])}
   *       multiple times (esp. in transaction that changes a lot of rows)
   *
   * @param row row to write to
   * @param columns columns to write to
   * @param values array of values to write (same order as values)
   */
  void put(byte[] row, byte[][] columns, byte[][] values) throws Exception;

  /**
   * Writes the specified value for the specified column for the specified row.
   *
   * @param row row to write to
   * @param column column to write to
   * @param value to write
   */
  void put(byte[] row, byte[] column, byte[] value) throws Exception;

  /**
   * Deletes all columns of the specified row.
   * NOTE: depending on the implementation of this interface and use-case, calling this method may be much less
   *       efficient than calling same method with columns as parameters because it may always require round trip to
   *       persistent store
   *
   * @param row row to delete
   */
  void delete(byte[] row) throws Exception;

  /**
   * Deletes specified column of the specified row.
   *
   * @param row row to delete from
   * @param column column name to delete
   */
  void delete(byte[] row, byte[] column) throws Exception;

  /**
   * Deletes specified columns of the specified row.
   *
   * NOTE: depending on the implementation this may work faster than calling {@link #delete(byte[], byte[])}
   *       multiple times (esp. in transaction that changes a lot of rows)
   *
   * @param row row to delete from
   * @param columns names of columns to delete
   */
  void delete(byte[] row, byte[][] columns) throws Exception;

  /**
   * Increments (atomically) the specified row and columns by the specified amounts.
   *
   * @param row row which values to increment
   * @param column column to increment
   * @param amount amount to increment by
   * @return new value of the column
   */
  long incrementAndGet(byte[] row, byte[] column, long amount) throws Exception;

  /**
   * Increments (atomically) the specified row and columns by the specified amounts.
   * <p>
   * NOTE: depending on the implementation this may work faster than calling
   * {@link #incrementAndGet(byte[], byte[], long)} multiple times (esp. in transaction that changes a lot of rows).
   * <p>
   * NOTE: objects that are passed in parameters can be re-used by underlying implementation and present
   *       in returned data structures from this method.
   *
   * @param row row which values to increment
   * @param columns columns to increment
   * @param amounts amounts to increment columns by (same order as columns)
   * @return map of values of counters after the increments are performed, never null
   */
  Map<byte[], Long> incrementAndGet(byte[] row, byte[][] columns, long[] amounts) throws Exception;

  /**
   * Increments (atomically) the specified row and columns by the specified amounts, without returning the new value.
   *
   * @param row row which values to increment
   * @param column column to increment
   * @param amount amount to increment by
   */
  void increment(byte[] row, byte[] column, long amount) throws Exception;

  /**
   * Increments (atomically) the specified row and columns by the specified amounts, without returning the new values.
   *
   * NOTE: depending on the implementation this may work faster than calling
   * {@link #increment(byte[], byte[], long)} multiple times (esp. in transaction that changes a lot of rows)
   *
   * @param row row which values to increment
   * @param columns columns to increment
   * @param amounts amounts to increment columns by (same order as columns)
   */
  void increment(byte[] row, byte[][] columns, long[] amounts) throws Exception;

  /**
   * Compares-and-swaps (atomically) the value of the specified row and column
   * by looking for the specified expected value and if found, replacing with
   * the specified new value.
   *
   * @param row row to modify
   * @param column column to change
   * @param expectedValue expected value before change
   * @param newValue value to set
   * @return true if compare and swap succeeded, false otherwise (stored value is different from expected)
   */
  boolean compareAndSwap(byte[] row, byte[] column, byte[] expectedValue, byte[] newValue) throws Exception;

  /**
   * Scans table.
   * @param startRow start row inclusive. {@code null} means start from first row of the table
   * @param stopRow stop row exclusive. {@code null} means scan all rows to the end of the table
   * @return instance of {@link co.cask.cdap.api.dataset.table.Scanner}
   */
  Scanner scan(byte[] startRow, byte[] stopRow) throws Exception;

  /**
   * Gets splits of range of rows of the table.
   * @param numSplits number of splits to return
   * @param startRow start row of the range, inclusive
   * @param stopRow stop row of the range, exclusive
   * @return list of {@link co.cask.cdap.api.data.batch.Split}s
   */
  List<Split> getSplits(int numSplits, byte[] startRow, byte[] stopRow) throws Exception;
}
