package com.continuuity.data.table;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.executor.ReadPointer;

import java.util.Map;

/**
 * A core table interface that supports columnar and versioned tables.
 *
 * Columnar implies that a row can have any number of dynamically inserted
 * columns (column-oriented).  Columns are sorted in ascending binary order.
 *
 * Versioned implies that every row+column can have multiple versions and these
 * can be used with a {@link com.continuuity.data.operation.executor.ReadPointer} to provide visibility constraint
 * possibilities (ie. for use with a transactional system).
 */
public interface VersionedColumnarTable {

  /**
   * Writes the specified value at the specified version for the specified
   * row and column.
   */
  public void put(byte [] row, byte [] column, long version, byte [] value) throws OperationException;

  /**
   * Writes the specified values for the specified columns at the specified
   * version for the specified row.
   */
  public void put(byte [] row, byte [][] columns, long version, byte[][] values) throws OperationException;

  /**
   * Writes values[i] at (rows[i], columns[i]) using the specified version
   * version for the specified rows.
   */
  public void put(byte [][] rows, byte [][] columns, long version, byte[][] values)
    throws OperationException;


  /**
   * Equivalent to put(rows[i], columnsPerRow[i], version, valuesPerRow[i]) for all i, in a single operation.
   * The number/set of columns can vary from row to row.
   * @param rows array of row keys
   * @param columnsPerRow for each row, the array of columns to put
   * @param version the write version
   * @param valuesPerRow for each row, the array of values to put
   */
  public void put(byte[][] rows, byte[][][] columnsPerRow, long version, byte[][][] valuesPerRow)
    throws OperationException;

  /**
   * Deletes the specified version of the specified row and column.
   */
  public void delete(byte [] row, byte [] column, long version) throws OperationException;

  /**
   * Deletes the specified version of the specified row and columns.
   */
  public void delete(byte [] row, byte [][] columns, long version) throws OperationException;

  /**
   * Deletes all versions of the specified row and column that have a version
   * less than or equal to the specified version.
   */
  public void deleteAll(byte [] row, byte [] column, long version) throws OperationException;

  /**
   * Deletes all versions of the specified row and columns that have a version
   * less than or equal to the specified version.
   */
  public void deleteAll(byte [] row, byte [][] columns, long version) throws OperationException;

  /**
   * Deletes all versions of the specified row and columns that have a version
   * less than or equal to the specified version. If the implementation supports it
   * this delete will affect all readers, even those with a read pointer less than
   * the given version, and it cannot be undone.
   */
  public void deleteDirty(byte [] row, byte [][] columns, long version) throws OperationException;

  /**
   * Deletes all versions of all columns of the specified rows. If the implementation supports it
   * this delete will affect all readers, and it cannot be undone.
   */
  public void deleteDirty(byte [][] rows) throws OperationException;

  /**
   * Undeletes (invalidates) a previously executed
   * {@link #deleteAll(byte[], byte[], long)} operation.
   */
  public void undeleteAll(byte [] row, byte [] column, long version) throws OperationException;

  /**
   * Undeletes (invalidates) a previously executed
   * {@link #deleteAll(byte[], byte[][], long)} operation.
   */
  public void undeleteAll(byte [] row, byte [][] columns, long version) throws OperationException;

  /**
   * Reads the latest version of all columns in the specified row, utilizing
   * the specified read pointer to enforce visibility constraints.
   * @return map of columns to values
   */
  public OperationResult<Map<byte [], byte []>>
  get(byte [] row, ReadPointer readPointer) throws OperationException;

  /**
   * Reads the latest version of the specified column in the specified row,
   * utilizing the specified read pointer to enforce visibility constraints,
   * and returns the value.
   * @return value of the latest visible column in the specified row, or null if
   *         none exists
   */
  public OperationResult<byte[]> get(byte [] row, byte [] column,
                                     ReadPointer readPointer) throws OperationException;

  /**
   * Reads the latest version of the specified column in the specified row,
   * utilizing the specified read pointer to enforce visibility constraints,
   * and returns both the value as well as the version this value exists at.
   * @return value and version of the latest visible column in the specified
   *         row, or null if none exists
   */
  public OperationResult<ImmutablePair<byte[], Long>> getWithVersion(
      byte[] row, byte[] column,
      ReadPointer readPointer) throws OperationException;

  /**
   * Reads the latest versions of all columns in the specified row that are
   * between the specified start (inclusive) and stop (exclusive) columns,
   * utilizing the specified read pointer to enforce visibility constraints.
   * @param startColumn beginning of range of columns, inclusive
   * @param stopColumn end of range of columns, exclusive
   * @param limit maximum number of columns to return
   * @return map of columns to values, never null
   */
  public OperationResult<Map<byte [], byte []>> get(
      byte [] row, byte[] startColumn, byte[] stopColumn,
      int limit, ReadPointer readPointer) throws OperationException;

  /**
   * Reads the latest versions of the specified columns in the specified row,
   * utilizing the specified read pointer to enforce visibility constraints.
   * @return map of columns to values, never null
   */
  public OperationResult<Map<byte[], byte[]>> get(
      byte [] row, byte[][] columns,
      ReadPointer readPointer) throws OperationException;

  /**
   * Reads the latest version of the specified column in the specified row,
   * utilizing the specified read pointer to enforce visibility constraints,
   * and returns the value.
   * @return value of the latest visible column in the specified row, or null if
   *         none exists
   */
  public OperationResult<byte[]> getDirty(
    byte [] row, byte [] column)
    throws OperationException;

  /**
   * Reads the latest versions of all specified columns for each row,
   * utilizing the specified read pointer to enforce visibility constraints.
   * @return map of columns to values, never null
   */
  public OperationResult<Map<byte[], Map<byte[], byte[]>>> getAllColumns(
    byte[][] rows, byte[][] columns, ReadPointer readPointer)
    throws OperationException;

  /**
   * Increments (atomically) the specified row and column by the specified
   * amount, utilizing the specified read pointer to enforce visibility
   * constraints when performing the initial read.  The specified write version
   * will be used when performing the post-incremented write.
   * @param amount amount to increment column by
   * @return value of counter after this increment is performed
   */
  public long increment(
      byte [] row, byte[] column, long amount,
      ReadPointer readPointer, long writeVersion)
    throws OperationException;

  /**
   * Increments (atomically and dirtily) the specified row and column by the specified
   * amount. It does the increment using dirty read and dirty write pointers. The operation
   * either increments the current value for the given row and column or inserts a new
   * row and column with the provided amount
   *
   * Important: Counters written with this method cannot be read with a regular get(), they
   * can only be read using this same method with an increment of 0.
   *
   * @param amount amount to increment column by
   * @return value of counter after this increment is performed
   */
  public long incrementAtomicDirtily(
    byte [] row, byte[] column, long amount)
    throws OperationException;

  // TODO implement a readDirtyCounter()

  /**
   * Increments (atomically) the specified row and columns by the specified
   * amounts, utilizing the specified read pointer to enforce visibility
   * constraints when performing the initial reads.  The specified write version
   * will be used when performing the post-incremented writes.
   * @param amounts amounts to increment columns by
   * @return values of counters after the increments are performed, never null
   */
  public Map<byte[], Long> increment(
    byte [] row, byte[][] columns, long[] amounts,
    ReadPointer readPointer, long writeVersion)
    throws OperationException;

  /**
   * Compares-and-swaps (atomically) the value of the specified row and column
   * by looking for the specified expected value and if found, replacing with
   * the specified new value.  Utilizes the specified read pointer to enforce
   * visibility constraints on the read, utilizes the specified write version
   * to perform the swap.
   *
   * @throws OperationException when there is a write conflict, i.e., expectedValue does not match existingValue.
   */
  public void compareAndSwap(byte[] row, byte[] column,
                             byte[] expectedValue, byte[] newValue,
                             ReadPointer readPointer, long writeVersion)
      throws OperationException;

  /**
   * Compares-and-swaps (atomically) the value of the specified row and column
   * by looking for the specified expected value and if found, replacing with
   * the specified new value. If the newValue is null, then the cell is deleted.
   * It does the compare and swap using dirty read and dirty write pointers.
   * It also assumes the values do not have tombstones.
   *
   * @param row byte representation of existing row to be swapped
   * @param column byte representation of column to be swapped
   * @param expectedValue byte representation of expected value in (row, col)
   * @param newValue byte representation of new value to be updated in (row, col)
   * @return true if swap was executed, false otherwise
   * @throws OperationException Note: this does not throw exception when
   * expectedValue does not match existingValue.
   */
  public boolean compareAndSwapDirty(byte[] row, byte[] column, byte[] expectedValue, byte[] newValue)
    throws OperationException;

  /**
   * Clears this table, completely wiping all data irrecoverably.
   */
  public void clear() throws OperationException;

}
