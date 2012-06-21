package com.continuuity.api.data;

import java.util.Map;


public class Increment implements WriteOperation, ReadOperation<Map<byte[],Long>> {

  private final byte [] key;
  private final byte [][] columns;
  private final long [] amounts;

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

  @Override
  public int getPriority() {
    return 1;
  }
}
