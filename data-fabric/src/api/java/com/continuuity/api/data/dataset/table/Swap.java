package com.continuuity.api.data.dataset.table;

/**
 * Compare a column of a row with an expected value. If the value matches,
 * write a new value to the column. It it does not match, the operation
 * fails.
 */
public class Swap extends AbstractWriteOperation {

  // the column to swap
  protected byte[] column;
  // the expected value
  protected byte[] expected;
  // the new value
  protected byte[] value;

  /**
   * Get the column to compare and swap.
   * @return the column key
   */
  public byte[] getColumn() {
    return column;
  }

  /**
   * Get the expected value for that column. If this is null,
   * then the column must not exist for the operation to succeed.
   * @return the expected value
   */
  public byte[] getExpected() {
    return expected;
  }

  /**
   * Get the new value for the column. If this returns null,
   * then the column will be deleted in case of success.
   * @return the new value
   */
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
