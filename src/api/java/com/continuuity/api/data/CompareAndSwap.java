package com.continuuity.api.data;


/**
 * Atomic compare-and-swap operation.
 * 
 * Performs an atomic compare-and-swap of the value of a key or column.  An
 * expected value and a new value are specified, and if the current value is
 * equal to the expected value, it is atomically replaced with the new value,
 * and the operation is successful.  If the current value was not equal to the
 * expected value, no change is made and the operation fails.
 * 
 * Supports key-value and columnar operations.
 */
public class CompareAndSwap implements ConditionalWriteOperation {

  /** The key/row */
  private final byte [] key;

  /** The column, if a columnar operation */
  private final byte [] column;

  /** The expected value */
  private final byte [] expectedValue;

  /** The new value */
  private final byte [] newValue;

  /**
   * Compares-and-swaps the value of the specified key by atomically comparing
   * if the current value is the specified expected value and if so, replacing
   * it with the specified new value.
   * 
   * @param key
   * @param expectedValue
   * @param newValue
   */
  public CompareAndSwap(final byte [] key, final byte [] expectedValue,
      final byte [] newValue) {
    this(key, KV_COL, expectedValue, newValue);
  }

  /**
   * Compares-and-swaps the value of the specified column in the specified row
   * by atomically comparing if the current value is the specified expected
   * value and if so, replacing it with the specified new value.
   * 
   * @param row
   * @param column
   * @param expectedValue
   * @param newValue
   */
  public CompareAndSwap(final byte [] row, final byte [] column,
      final byte [] expectedValue, final byte [] newValue) {
    this.key = row;
    this.column = column;
    this.expectedValue = expectedValue;
    this.newValue = newValue;
  }

  @Override
  public byte [] getKey() {
    return this.key;
  }

  public byte [] getColumn() {
    return this.column;
  }

  public byte [] getExpectedValue() {
    return this.expectedValue;
  }

  public byte [] getNewValue() {
    return this.newValue;
  }

  @Override
  public int getPriority() {
    return 1;
  }
}
