package com.continuuity.api.data.dataset.table;

/**
 * A write to a table. It can write one more columns of a row
 */
public class Write extends AbstractWriteOperation {
  protected byte[][] columns;
  protected byte[][] values;

  /** get the columns to write */
  public byte[][] getColumns() {
    return columns;
  }

  /** get the values to write */
  public byte[][] getValues() {
    return values;
  }

  /**
   * Write several columns. columns must have exactly the same length as
   * values, such that values[i] will be written to columns[i] of the row.
   * @param row a row key
   * @param columns an array of column keys
   * @param values an array of values to be written
   */
  public Write(byte[] row, byte[][] columns, byte[][] values) {
    super(row);
    this.columns = columns;
    this.values = values;
  }

  /**
   * Write a value to one column
   * @param row a row key
   * @param column a column key
   * @param value a new value for the column
   */
  public Write(byte[] row, byte[] column, byte[] value) {
    this(row, new byte[][] { column }, new byte[][] { value });
  }
}

