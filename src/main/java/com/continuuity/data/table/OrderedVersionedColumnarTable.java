package com.continuuity.data.table;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.KeyRange;
import com.continuuity.data.operation.executor.ReadPointer;

import java.util.List;


public interface OrderedVersionedColumnarTable extends VersionedColumnarTable {

  /**
   * Scans the table and returns all row keys according to the specified
   * limit and offset and respecting the specified read pointer.
   * @param limit
   * @param offset
   * @param readPointer
   * @return list of keys
   */
  public List<byte[]> getKeys(int limit, int offset, ReadPointer readPointer) throws OperationException;

  /**
   * Scans all columns of all rows between the specified start row (inclusive)
   * and stop row (exclusive).  Returns the latest visible version of each
   * column.
   * @param startRow
   * @param stopRow
   * @param readPointer
   * @return scanner cursor
   */
  public Scanner scan(byte[] startRow, byte[] stopRow,
      ReadPointer readPointer);


  /**
   * Scans the specified columns of all rows between the specified start row
   * (inclusive) and stop row (exclusive).  Returns the latest visible version
   * of each column.
   * @param startRow
   * @param stopRow
   * @param columns
   * @param readPointer
   * @return scanner cursor
   */
  public Scanner scan(byte[] startRow, byte[] stopRow,
      byte[][] columns, ReadPointer readPointer);


  /**
   * Scans all columns of all rows.  Returns the latest visible version of each
   * column.
   * @param readPointer
   * @return scanner cursor
   */
  public Scanner scan(ReadPointer readPointer);

  /**
   * Gets the value associated with the least key that is greater than or equal to the given row for
   * the specified column. Returns empty OperationResult if there is no such value.
   *
   * @param row
   * @param column
   * @param readPointer
   * @return Value  of the column that corresponds to the least key that is greater than or equal to
   * given row. Returns empty OperationResult if there is no such value. Never returns null.
   * @throws OperationException
   */
  public OperationResult<byte[]> getCeilValue(byte[] row, byte[] column, ReadPointer
    readPointer) throws OperationException;

  /**
   * Returns a list of key ranges that partition the table into approximately evenly sized ranges. If a start or a
   * stop row key are given, then they limit the range covered by the partitions. If a list of columns is given,
   * then they are a hint for the partitioning that only these columns will be read. If a number of splits is
   * explicitly requested, then the number of returned ranges will not exceed that number.
   * @param numSplits the desired number of splits, or -1 to leave it up to the partitioner
   * @param start the start of the range to be covered by the splits, or null to start with the least key in the table
   * @param stop the end of the range to be covered by the splits, exclusively, or null to read to the end of the table
   * @param columns if non-null, a hint for the partitioner that only these columns are needed.
   * @param pointer the read pointer under which splits should be visible.
   * @return A list of key ranges.
   * @throws OperationException in case of error
   */
  public List<KeyRange> getSplits(int numSplits, byte[] start, byte[] stop, byte[][] columns, ReadPointer pointer)
    throws OperationException;

  /**
   * Gets the value and version associated with the least key that is less than or equal to the given row.
   * Returns null if there is no such key
   * @param row
   * @param column
   * @param readPointer
   * @return Value and version of the column that corresponds to the least key that is less than or equal to
   * given row. Null if no such key
   * @throws OperationException
   */
//  public OperationResult<ImmutablePair<byte[], Long>> getFloorValueWithVersion( byte[] row, byte[] column,
//                                                              ReadPointer readPointer) throws OperationException;

}
