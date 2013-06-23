package com.continuuity.api.data.dataset.table;

/**
 * A Read operation on a table. Reads either a list or a range of columns from
 * one row of the table.
 */
public class Read {
  // the row key of the row to read
  protected byte[] row;
  // the column keys of the columns to read
  protected byte[][] columns;
  // the start of the column range to read
  protected byte[] startCol;
  // the end (exclusive) of the column range
  protected byte[] stopCol;
  // the max number of columns to return
  protected int limit;

  /**
   * Get the row key.
   * @return the row key of the operation
   */
  public byte[] getRow() {
    return row;
  }

  /**
   * Get the columns to read. If this returns null, then the read is for a
   * range of columns.
   * @return the keys of the columns to read.
   */
  public byte[][] getColumns() {
    return columns;
  }

  /**
   * Get the start of the column range to read.
   * @return the start of the column range
   */
  public byte[] getStartCol() {
    return startCol;
  }

  /**
   * Get the end (exclusive) of the column range to read.
   * @return the end of the column range
   */
  public byte[] getStopCol() {
    return stopCol;
  }

  /**
   * Get the limit for the number of columns to return.
   * @return the limit for the number of columns to return
   */
  public int getLimit() {
    return limit;
  }

  /**
   * Read several columns.
   * @param row the row key
   * @param columns an array of column keys
   */
  public Read(byte[] row, byte[][] columns) {
    this.row = row;
    this.columns = columns;
    this.startCol = this.stopCol = null;
    this.limit = -1;
  }

  /**
   * Read a single column.
   * @param row the row key
   * @param column the column key
   */
  public Read(byte[] row, byte[] column) {
    this(row, new byte[][] { column });
  }

  /**
   * Read a consecutive range of columns.
   * @param row the row key
   * @param start the column to read. If null, reading will start with the
   *              first column of the row.
   * @param stop the first column to exclude from the read. If null,
   *             the read will end with the last column in the row.
   */
  public Read(byte[] row, byte[] start, byte[] stop) {
    this(row, start, stop, -1);
  }

  /**
   * Read all columns of a row.
   * @param row the row key
   */
  public Read(byte[] row) {
    this(row, -1);
  }

  /**
   * Read all columns of a row to a limited number.
   * @param row the row key
   * @param limit a limit for the number of columns to return. A value -1
   *              signifies unlimited.
   */
  public Read(byte[] row, int limit) {
    this(row, null, null, limit);
  }

  /**
   * Read a consecutive range of columns up to a limited number.
   * @param row the row key
   * @param start the column to read. If null, reading will start with the
   *              first column of the row.
   * @param stop the first column to exclude from the read. If null,
   *             the read will end with the last column in the row.
   * @param limit a limit for the number of columns to return. A value -1
   *              signifies unlimited.
   */
  public Read(byte[] row, byte[] start, byte[] stop, int limit) {
    this.row = row;
    this.columns = null;
    this.startCol = start;
    this.stopCol = stop;
    this.limit = limit;
  }
}
