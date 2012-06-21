package com.continuuity.data.table;

import java.util.List;


public interface OrderedVersionedColumnarTable extends VersionedColumnarTable {

  /**
   * Scans the table and returns all row keys according to the specified
   * limit and offset and respecting the specified read pointer.
   * @param limit
   * @param offset
   * @param readPointer
   */
  public List<byte[]> getKeys(int limit, int offset, ReadPointer readPointer);

  /**
   * Scans all columns of all rows between the specified start row (inclusive)
   * and stop row (exclusive).  Returns the latest visible version of each
   * column.
   * @param startRow
   * @param stopRow
   * @param readPointer
   */
  public Scanner scan(byte[] startRow, byte[] stopRow,
      ReadPointer readPointer);


  /**
   * Scans the specified columns of all rows between the specified start row
   * (inclusive) and stop row (exclusive).  Returns the latest visible version
   * of each column.
   * @param startRow
   * @param stopRow
   * @param readPointer
   */
  public Scanner scan(byte[] startRow, byte[] stopRow,
      byte[][] columns, ReadPointer readPointer);


  /**
   * Scans all columns of all rows.  Returns the latest visible version of each
   * column.
   * @param readPointer
   */
  public Scanner scan(ReadPointer readPointer);
}
