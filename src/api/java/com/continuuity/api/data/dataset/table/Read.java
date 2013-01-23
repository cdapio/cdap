package com.continuuity.api.data.dataset.table;

/**
 * A Read operation on a table. Reads either a list or a range of columns from
 * one row of the table.
 */
public class Read {
  protected byte[] row;
  protected byte[][] columns;
  protected byte[] startCol;
  protected byte[] stopCol;

  /** get the row key */
  public byte[] getRow() {
    return row;
  }

  /** get the columns to read */
  public byte[][] getColumns() {
    return columns;
  }

  /** get the start of the column range to read */
  public byte[] getStartCol() {
    return startCol;
  }

  /** get the end (exclusive) of the column range to read */
  public byte[] getStopCol() {
    return stopCol;
  }

  /**
   * Read a several columns
   * @param row the row key
   * @param columns an array of column keys
   */
  public Read(byte[] row, byte[][] columns) {
    this.row = row;
    this.columns = columns;
    this.startCol = this.stopCol = null;
  }

  /**
   * Read a single column
   * @param row the row key
   * @param column the column key
   */
  public Read(byte[] row, byte[] column) {
    this(row, new byte[][] { column });
  }

  /**
   * Read a consecutive range of columns
   * @param row the row key
   * @param start the column to read. If null, reading will start with the
   *              first column of the row.
   * @param stop the first column to exclude from the read. If null,
   *             the read will end with the last column in the row.
   */
  public Read(byte[] row, byte[] start, byte[] stop) {
    this.row = row;
    this.columns = null;
    this.startCol = start;
    this.stopCol = stop;
  }
}