package com.continuuity.api.data.dataset.table;

/**
 * Compare a column of a row with an expected value. If the value matches,
 * write a new value to the columns. It it does not match, the operation
 * fails.
 */
public class Swap extends WriteOperation {

  protected byte[] column;
  protected byte[] expected;
  protected byte[] value;

  /** get  the column to compare and swap */
  public byte[] getColumn() {
    return column;
  }

  /** get the expected value for that column */
  public byte[] getExpected() {
    return expected;
  }

  /** get the new value for the column */
  public byte[] getValue() {
    return value;
  }

  /**
   * Swap constructor.
   * @param row the row key
   * @param column the column key of the column to be swapped
   * @param expected the expected value. If null, then the existing value of
   *                 the column must be null, or the column must not exist.
   * @param value the new value. If null, then the column is deleted.
   */
  public Swap(byte[] row, byte[] column, byte[] expected, byte[] value) {
    super(row);
    this.column = column;
    this.expected = expected;
    this.value = value;
  }
}