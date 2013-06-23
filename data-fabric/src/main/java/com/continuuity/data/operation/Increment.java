package com.continuuity.data.operation;

import com.google.common.base.Objects;

/**
 * Atomic increment operation.
 *
 * Performs increments of 8 byte (long) keys and columns.  The increment is
 * performed atomically and the post-incremented value is returned.
 *
 * Supports key-value and columnar operations.
 */
public class Increment extends WriteOperation implements TableOperation {

  // the name of the table
  private final String table;

  // The key/row
  private final byte [] key;

  // The columns to be incremented
  private final byte [][] columns;

  // The amounts to increment the columns by
  private final long [] amounts;

  /**
   * Increments the specified column in the specified row by the specified
   * amount, in the default table
   *
   * This is a columnar operation.
   *
   * @param row the row key to increment for
   * @param column the column to increment
   * @param amount the amount to increment by
   */
  public Increment(final byte[] row,
                   final byte[] column,
                   final long amount) {
    this(null, row, column, amount);
  }

  /**
   * Increments the specified column in the specified row by the specified
   * amount, in the specified table
   *
   * This is a columnar operation.
   *
   * @param table the table to increment in
   * @param row the row key to increment for
   * @param column the column to increment
   * @param amount the amount to increment by
   */
  public Increment(final String table,
                   final byte [] row,
                   final byte [] column,
                   final long amount) {
    this(table, row, new byte [][] { column }, new long [] { amount });
  }

  /**
   * Increments the specified columns in the specified row by the specified
   * amounts, in the default table
   *
   * This is a columnar operation.
   *
   * @param row the row key to increment for
   * @param columns the columns to increment
   * @param amounts the amounts to increment, in the same order as the columns
   */
  public Increment(final byte [] row,
                   final byte [][] columns,
                   final long [] amounts) {
    this(null, row, columns, amounts);
  }

  /**
   * Increments the specified columns in the specified row by the specified
   * amounts, in the specified table
   *
   * This is a columnar operation.
   *
   * @param table the table to increment in
   * @param row the row key to increment for
   * @param columns the columns to increment
   * @param amounts the amounts to increment, in the same order as the columns
   */
  public Increment(final String table,
                   final byte [] row,
                   final byte [][] columns,
                   final long [] amounts) {
    checkColumnArgs(columns, amounts);
    this.table = table;
    this.key = row;
    this.columns = columns;
    this.amounts = amounts;
  }

  /**
   * Increments the specified columns in the specified row by the specified
   * amounts, in the specified table
   *
   * This is a columnar operation.
   *
   * @param id the unique id of this operation
   * @param table the table to increment in
   * @param row the row key to increment for
   * @param columns the columns to increment
   * @param amounts the amounts to increment, in the same order as the columns
   */
  public Increment(final long id,
                   final String table,
                   final byte [] row,
                   final byte [][] columns,
                   final long [] amounts) {
    super(id);
    checkColumnArgs(columns, amounts);
    this.table = table;
    this.key = row;
    this.columns = columns;
    this.amounts = amounts;
  }

  @Override
  public String getTable() {
    return this.table;
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
  public int getPriority() {
    return 1;
  }

  /**
   * Checks the specified columns and amounts arguments for validity.
   * @param columns the columns to increment
   * @param amounts the amounts to increment, in the same order as the columns
   * @throws IllegalArgumentException if no columns specified, no amounts
   *    specified, or number of columns does not match number of amounts.
   */
  public static void checkColumnArgs(final Object [] columns,
      final long [] amounts) {
    if (columns == null || columns.length == 0) {
      throw new IllegalArgumentException("Must contain at least one column");
    }
    if (amounts == null || amounts.length == 0) {
      throw new IllegalArgumentException("Must contain at least one amount");
    }
    if (columns.length != amounts.length) {
      throw new IllegalArgumentException(
        "Number of columns (" + columns.length + ") does not match number of amounts (" + amounts.length + ")");
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("Increment '");
    builder.append(new String(this.getKey()));
    builder.append("':");
    for (int i = 0; i < this.columns.length; i++) {
      builder.append(" '");
      builder.append(new String(this.columns[i]));
      builder.append("'+=");
      builder.append(this.amounts[i]);
    }
    return builder.toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(key, columns, amounts);
  }

  @Override
  public int getSize() {
    if (key == null || columns == null) {
      return 0;
    }
    int size = key.length;
    for (int i = 0; i < columns.length; i++) {
      size += columns[i].length + 8;
    }
    return size;
  }
}
