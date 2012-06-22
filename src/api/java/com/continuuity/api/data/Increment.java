package com.continuuity.api.data;

import java.util.Map;

/**
 * Atomic increment operation.
 * 
 * Performs increments of 8 byte (long) keys and columns.  The increment is
 * performed atomically and the post-incremented value is returned.
 * 
 * Supports key-value and columnar operations.
 */
public class Increment
implements WriteOperation, ReadOperation<Map<byte[],Long>> {

  /** The key/row */
  private final byte [] key;
  
  /** The columns to be incremented */
  private final byte [][] columns;
  
  /** The amounts to increment the columns by */
  private final long [] amounts;

  /** The values of the counters after being incremented */
  private Map<byte[],Long> incrementedValues;

  /**
   * Increments the specified key by the specified amount.
   *
   * This is a key-value operation.
   *
   * @param key
   * @param amount
   */
  public Increment(final byte [] key, long amount) {
    this(key, KV_COL_ARR, new long [] { amount });
  }

  /**
   * Increments the specified column in the specified row by the specified
   * amount.
   *
   * This is a columnar operation.
   *
   * @param row
   * @param column
   * @param amount
   */
  public Increment(final byte [] row, final byte [] column, final long amount) {
    this(row, new byte [][] { column }, new long [] { amount });
  }

  /**
   * Increments the specified columns in the specified row by the specified
   * amounts.
   *
   * This is a columnar operation.
   *
   * @param row
   * @param columns
   * @param amounts
   */
  public Increment(final byte [] row, final byte [][] columns,
      final long [] amounts) {
    checkColumnArgs(columns, amounts);
    this.key = row;
    this.columns = columns;
    this.amounts = amounts;
  }

  @Override
  public byte [] getKey() {
    return this.key;
  }

  public long [] getAmounts() {
    return this.amounts;
  }

  public byte [][] getColumns() {
    return this.columns;
  }

  @Override
  public void setResult(Map<byte[],Long> incrementedValues) {
    this.incrementedValues = incrementedValues;
  }

  @Override
  public Map<byte[],Long> getResult() {
    return this.incrementedValues;
  }

  /**
   * Returns the post-incremented value of a key (not column) increment.
   * @return the long value of the key counter after being incremented
   */
  public Long getKeyResult() {
    return this.incrementedValues.get(KV_COL);
  }

  @Override
  public int getPriority() {
    return 1;
  }

  /**
   * Checks the specified columns and amounts arguments for validity.
   * @param columns
   * @param amounts
   * @throws IllegalArgumentException if no columns specified
   * @throws IllegalArgumentException if no amounts specified
   * @throws IllegalArgumentException if number of columns does not match number
   *                                  of amounts
   */
  public static void checkColumnArgs(final Object [] columns,
      final long [] amounts) {
    if (columns == null || columns.length == 0)
      throw new IllegalArgumentException("Must contain at least one column");
    if (amounts == null || amounts.length == 0)
      throw new IllegalArgumentException("Must contain at least one amount");
    if (columns.length != amounts.length)
      throw new IllegalArgumentException("Number of columns (" +
          columns.length + ") does not match number of amounts (" +
          amounts.length + ")");
  }
}
