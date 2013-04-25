package com.continuuity.data.table;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
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
   * Scans the table and returns all row keys according to the specified
   * limit.
   * @param limit
   * @return list of keys
   */
  public List<byte[]> getKeysDirty(int limit) throws OperationException;

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
   * @param row  Start row for the scan
   * @param column column value to get
   * @param readPointer instance of ReadPointer
   * @return Value  of the column that corresponds to the least key that is greater than or equal to
   * given row. Returns empty OperationResult if there is no such value. Never returns null.
   * @throws OperationException
   */
  public OperationResult<byte[]> getCeilValue(byte[] row, byte[] column, ReadPointer
    readPointer) throws OperationException;

  /**
   * Gets the value associated with the least key that is greater than or equal to the given row for the
   * specified column.
   * @param row  Start row for the scan
   * @param column column value to get
   * @return Value  of the column that corresponds to the least key that is greater than or equal to
   * given row. Returns empty OperationResult if there is no such value. Never returns null.
   * @throws OperationException
   */
  public OperationResult<byte[]> getCeilValueDirty(byte[] row, byte[] column) throws OperationException;

  /**
   * Scans all columns of all rows between the specified start row (inclusive)
   * and stop row (exclusive).  This scan is dirty - doesn't use read pointer
   * column.
   * @param startRow
   * @param stopRow
   * @return scanner cursor
   */
  public Scanner scanDirty(byte[] startRow, byte[] stopRow);
}
