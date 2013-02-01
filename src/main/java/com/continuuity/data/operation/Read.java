package com.continuuity.data.operation;

import com.google.common.base.Objects;

/**
 * Read the value of a key or the values of columns.
 *
 * Supports both key-value and columnar operations.
 */
public class Read implements ReadOperation {

  /** Unique id for the operation */
  private final long id;

  /** the name of the table */
  private final String table;

  /** The key/row to read */
  private final byte [] key;

  /** The columns to read */
  private final byte [][] columns;

  /**
   * Reads the value of the specified key from the default table.
   *
   * @param key the key to read
   */
  public Read(final byte [] key) {
    this((String)null, key);
  }

  /**
   * Reads the value of the specified key from the specified table.
   *
   * @param table the name of the table to read from
   * @param key the key to read
   */
  public Read(final String table,
              final byte [] key) {
    this(table, key, KV_COL_ARR);
  }

  /**
   * Reads the value of the specified column in the specified row,
   * from the default table.
   *
   * @param row the row to be read
   * @param column the columns to be read
   */
  public Read(final byte [] row, final byte [] column) {
    this(null, row, column);
  }

  /**
   * Reads the value of the specified column in the specified row,
   * from a specified table.
   *
   * @param table the name of the table to read from
   * @param row the row to be read
   * @param column the columns to be read
   */
  public Read(final String table,
              final byte [] row,
              final byte [] column) {
    this(table, row, new byte [][] { column } );
  }

  /**
   * Reads the values of the specified columns in the specified row,
   * from the default table.
   *
   * @param row the row to be read
   * @param columns the columns to be read
   */
  public Read(final byte [] row, final byte [][] columns) {
    this(null, row, columns);
  }

  /**
   * Reads the values of the specified columns in the specified row,
   * from a specified table.
   *
   * @param table the name of the table to read from
   * @param row the row to be read
   * @param columns the columns to be read
   */
  public Read(final String table,
              final byte [] row,
              final byte [][] columns) {
    this(OperationBase.getId(), table, row, columns);
  }

  /**
   * Reads the values of the specified columns in the specified row,
   * from a specified table.
   *
   * @param id explicit unique id of this operation
   * @param table the name of the table to read from
   * @param row the row to be read
   * @param columns the columns to be read
   */
  public Read(final long id,
              final String table,
              final byte [] row,
              final byte [][] columns) {
    this.id = id;
    this.table = table;
    this.key = row;
    this.columns = columns;
  }

  public String getTable() {
    return this.table;
  }

  public byte [] getKey() {
    return this.key;
  }

  public byte [][] getColumns() {
    return this.columns;
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    char sep = '[';
    for (byte[] column : this.columns) {
      builder.append(sep);
      builder.append(new String(column));
      sep = ',';
    }
    builder.append(']');
    String columnsStr = builder.toString();
    return Objects.toStringHelper(this)
        .add("key", new String(this.key))
        .add("columns", columnsStr)
        .toString();
  }

  @Override
  public long getId() {
    return id;
  }
}
