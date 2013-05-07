package com.continuuity.api.data.dataset.table;

/**
 * An Increment interprets the values of columns as 8-byte integers, and
 * increments them by given value. The operation fails if a column's
 * existing value is not exactly 8 bytes long. If one of the columns to
 * increment does not exist prior to the operation, then it will be set to
 * the value to increment.
 */
public class Increment extends AbstractWriteOperation {
  // the columns to be incremented
  protected byte[][] columns;
  // the values to increment by
  protected long[] values;

  /**
   * Get the columns to increment.
   * @return the column keys of the columns to be incremented
   */
  public byte[][] getColumns() {
    return columns;
  }

  /**
   * Get the increment values.
   * @return the values by which each row is to be incremented
   */
  public long[] getValues() {
    return values;
  }

  /**
   * Increment several columns. columns must have exactly the same length as
   * values, such that the column with key columns[i] will be incremented
   * by values[i].
   * @param row the row key
   * @param columns the columns keys
   * @param values the increment values
   */
  public Increment(byte[] row, byte[][] columns, long[] values) {
    super(row);
    this.columns = columns;
    this.values = values;
  }

  /**
   * Increment a single column.
   * @param row the row key
   * @param column the column key
   * @param value the value to add
   */
  public Increment(byte[] row, byte[] column, long value) {
    this(row, new byte[][] { column }, new long[] { value });
  }
}

