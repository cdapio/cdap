package com.continuuity.data2.dataset.lib.table;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.batch.Split;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.table.Scanner;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * todo: docs
 */
public interface OrderedColumnarTable {
  // empty result constant
  static final OperationResult<Map<byte[], byte[]>> EMPTY_RESULT =
    new OperationResult<Map<byte[], byte[]>>(StatusCode.KEY_NOT_FOUND);

  // empty immutable row's column->value map constant
  static final NavigableMap<byte[], byte[]> EMPTY_ROW_MAP =
    Maps.unmodifiableNavigableMap(Maps.<byte[], byte[], byte[]>newTreeMap(Bytes.BYTES_COMPARATOR));

  /**
   * Reads the latest versions of the specified columns in the specified row.
   * @return map of columns to values, never null
   */
  OperationResult<Map<byte[], byte[]>> get(byte [] row, byte[][] columns) throws Exception;

  /**
   * Reads the latest versions of all columns in the specified row that are
   * between the specified start (inclusive) and stop (exclusive) columns.
   * @param startColumn beginning of range of columns, inclusive
   * @param stopColumn end of range of columns, exclusive
   * @param limit maximum number of columns to return
   * @return map of columns to values, never null
   */
  OperationResult<Map<byte [], byte []>> get(byte [] row, byte[] startColumn, byte[] stopColumn, int limit)
    throws Exception;

  /**
   * Writes the specified values for the specified columns for the specified row.
   */
  void put(byte [] row, byte [][] columns, byte[][] values) throws Exception;

  /**
   * Deletes specified columns of the specified row.
   */
  void delete(byte [] row, byte [][] columns) throws Exception;

  /**
   * Increments (atomically) the specified row and columns by the specified
   * amounts
   * @param amounts amounts to increment columns by
   * @return values of counters after the increments are performed, never null
   */
  Map<byte[], Long> increment(byte [] row, byte[][] columns, long[] amounts) throws Exception;


  /**
   * Compares-and-swaps (atomically) the value of the specified row and column
   * by looking for the specified expected value and if found, replacing with
   * the specified new value.
   * todo: returns true if succeeded
   */
  boolean compareAndSwap(byte[] row, byte[] column, byte[] expectedValue, byte[] newValue) throws Exception;

  List<Split> getSplits(int numSplits, byte[] start, byte[] stop) throws Exception;

  Scanner scan(byte[] startRow, byte[] stopRow) throws Exception;
}
