package com.continuuity.data2.dataset.lib.table;

import com.continuuity.data.table.Scanner;
import com.continuuity.data2.OperationResult;

import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A table interface dedicated to our metrics system. It is non-transactional, hence does not implement read or write
 * isolation. It provides all functions that the metrics system uses for its data.
 */
public interface MetricsTable {

  /**
   * Read single column of a row
   */
  OperationResult<byte[]> get(byte[] row, byte[] column) throws Exception;

  /**
   * Write multiple rows, each with multiple individual columns to write.
   */
  void put(Map<byte[], Map<byte[], byte[]>> updates) throws Exception;

  /**
   * Atomically compare a single column of a row with a expected value, and if it matches, replace it with a new value.
   * @param oldValue the expected value of the column. If null, this means that the column must not exist.
   * @param newValue the new value of the column. If null, the effect to delete the column if the comparison succeeds.
   * @return whether the write happened, that is, whether the existing value of the column matched the expected value.
   */
  boolean swap(byte[] row, byte[] column, byte[] oldValue, byte[] newValue) throws Exception;

  /**
   * Atomic increment of multiple columns of a row. If a column does not exist prior to the increment,
   * its value is assumed zero. Increments are only guaranteed atomic with respect to other increments.
   * @param increments Map from each column key to the value it should be incremented by.
   */
  void increment(byte[] row, Map<byte[], Long> increments) throws Exception;

  /**
   * Increment a single column of a row and return the new value.
   * @return the new value after the increment.
   */
  long incrementAndGet(byte[] row, byte[] column, long delta) throws Exception;

  /**
   * Delete (all columns of) all rows that start with a given prefix.
   */
  void deleteAll(byte[] prefix) throws Exception;

  /**
   * Delete (all columns of) a set of rows.
   */
  void delete(Collection<byte[]> rows) throws Exception;

  /**
   * Scan the table from the start row to the stop row and delete all matching cells,
   * where the row and column match the given filter and column list.
   * @param start the row key of the first row to scan. If null, the scan begins at the first row of the table.
   * @param stop the row key of the last row to scan. If null, the scan goes to the last row of the table.
   * @param columns if non-null, delete only the given columns.
   * @param filter if non-null, a fuzzy row filter used to efficiently skip over entire rows.
   */
  void deleteRange(@Nullable byte[] start, @Nullable byte[] stop, @Nullable byte[][] columns,
                   @Nullable FuzzyRowFilter filter) throws Exception;


  /**
   * Get a scanner for a table.
   * @param start the row key of the first row to scan. If null, the scan begins at the first row of the table.
   * @param stop the row key of the last row to scan. If null, the scan goes to the last row of the table.
   * @param columns if non-null, return only the given columns.
   * @param filter if non-null, a fuzzy row filter used to efficiently skip over entire rows.
   */
  Scanner scan(@Nullable byte[] start, @Nullable byte[] stop, @Nullable byte[][] columns,
               @Nullable FuzzyRowFilter filter) throws Exception;

}

