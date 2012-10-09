/*
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.api.data;

import com.google.common.base.Objects;

import java.util.Arrays;

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

  /** Unique id for the operation */
  private final long id;

  /** the name of the table */
  private final String table;

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
   * it with the specified new value. This happens in the default table.
   *
   * @param key the key of the row to perform the operation on
   * @param expectedValue the expected value of the column
   * @param newValue the new value to write
   */
  public CompareAndSwap(final byte [] key, final byte [] expectedValue,
                        final byte [] newValue) {
    this((String)null, key, expectedValue, newValue);
  }

  /**
   * Compares-and-swaps the value of the specified key by atomically comparing
   * if the current value is the specified expected value and if so, replacing
   * it with the specified new value.
   *
   * @param table the table to perform the operation on
   * @param key the key of the row to perform the operation on
   * @param expectedValue the expected value of the column
   * @param newValue the new value to write
   */
  public CompareAndSwap(final String table,
                        final byte [] key,
                        final byte [] expectedValue,
                        final byte [] newValue) {
    this(table, key, KV_COL, expectedValue, newValue);
  }

  /**
   * Compares-and-swaps the value of the specified column in the specified row
   * by atomically comparing if the current value is the specified expected
   * value and if so, replacing it with the specified new value. This happens
   * in the default table.
   *
   * @param row the row to perform the operation on
   * @param column the column to compare and swap
   * @param expectedValue the expected value of the column
   * @param newValue the new value to write
   */
  public CompareAndSwap(final byte [] row,
                        final byte [] column,
                        final byte [] expectedValue,
                        final byte [] newValue) {
    this(null, row, column, expectedValue, newValue);
  }

  /**
   * Compares-and-swaps the value of the specified column in the specified row
   * by atomically comparing if the current value is the specified expected
   * value and if so, replacing it with the specified new value.
   *
   * @param table the table to perform the operation on
   * @param row the row to perform the operation on
   * @param column the column to compare and swap
   * @param expectedValue the expected value of the column
   * @param newValue the new value to write
   */
  public CompareAndSwap(final String table,
                        final byte [] row,
                        final byte [] column,
                        final byte [] expectedValue,
                        final byte [] newValue) {
    this(OperationBase.getId(), table, row, column, expectedValue, newValue);
  }

  /**
   * Compares-and-swaps the value of the specified column in the specified row
   * by atomically comparing if the current value is the specified expected
   * value and if so, replacing it with the specified new value.
   *
   * @param id explicit unique id of this operation
   * @param table the table to perform the operation on
   * @param row the row to perform the operation on
   * @param column the column to compare and swap
   * @param expectedValue the expected value of the column
   * @param newValue the new value to write
   */
  public CompareAndSwap(final long id,
                        final String table,
                        final byte [] row,
                        final byte [] column,
                        final byte [] expectedValue,
                        final byte [] newValue) {
    this.id = id;
    this.table = table;
    this.key = row;
    this.column = column;
    this.expectedValue = expectedValue;
    this.newValue = newValue;
  }

  public String getTable() {
    return this.table;
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

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("key", new String(this.key))
        .add("column", new String(this.column))
        .add("expected", Arrays.toString(this.expectedValue))
        .add("newValue", Arrays.toString(this.newValue))
        .toString();
  }

  @Override
  public long getId() {
    return id;
  }
}
