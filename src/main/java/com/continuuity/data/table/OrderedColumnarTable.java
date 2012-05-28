package com.continuuity.data.table;


public interface OrderedColumnarTable extends ColumnarTable {
  
  /**
   * Scans all columns of all rows between the specified start row (inclusive)
   * and stop row (exclusive).  Returns the latest version of each column.
   * @param startRow
   * @param stopRow
   * @return
   */
  public Scanner scan(byte[] startRow, byte[] stopRow);


  /**
   * Scans the specified columns of all rows between the specified start row
   * (inclusive) and stop row (exclusive).  Returns the latest version of each
   * column.
   * @param startRow
   * @param stopRow
   * @return
   */
  public Scanner scan(byte[] startRow, byte[] stopRow, byte[][] columns);


  /**
   * Scans all columns of all rows.  Returns the latest version of each column.
   * @param startRow
   * @param stopRow
   * @return
   */
  public Scanner scan();
}
