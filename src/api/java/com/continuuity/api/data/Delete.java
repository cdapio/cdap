package com.continuuity.api.data;

/**
 * Delete a key or columns.
 */
public class Delete implements WriteOperation {

  /** the name of the table */
  private final String table;

  /** The key/row */
  private final byte [] key;
  
  /** The columns to be deleted */
  private final byte [][] columns;

  /**
   * Deletes the specified key-value from the default table
   *
   * This is a key-value operation.
   *
   * @param key the key to delete
   */
  public Delete(final byte [] key) {
    this((String)null, key);
  }

  /**
   * Deletes the specified key-value from the specified table
   *
   * This is a key-value operation.
   *
   * @param table the name of the table to delete from
   * @param key the key to delete
   */
  public Delete(String table, final byte [] key) {
    this(table, key, KV_COL_ARR);
  }

  /**
   * Deletes the specified column in the specified row from the default table.
   *
   * This is a columnar operation.
   *
   * @param row the row containing the column to delete
   * @param column the column to delete
   */
  public Delete(final byte [] row,
                final byte [] column) {
    this(null, row, column);
  }

  /**
   * Deletes the specified column in the specified row from the specified table.
   *
   * This is a columnar operation.
   *
   * @param table the name of the table to delete from
   * @param row the row containing the column to delete
   * @param column the column to delete
   */
  public Delete(final String table,
                final byte [] row,
                final byte [] column) {
    this(table, row, new byte [][] { column });
  }

  /**
   * Deletes the specified columns in the specified row from the default table
   *
   * This is a columnar operation.
   *
   * @param row the row containing the columns to delete
   * @param columns the columns to delete
   */
  public Delete(final byte[] row,
                final byte [][] columns) {
    this(null, row, columns);
  }

  /**
   * Deletes the specified columns in the specified row from the specified table
   *
   * This is a columnar operation.
   *
   * @param table the name of the table to delete from
   * @param row the row containing the columns to delete
   * @param columns the columns to delete
   */
  public Delete(final String table,
                final byte [] row,
                final byte [][] columns) {
    this.table = table;
    this.key = row;
    this.columns = columns;
  }

  public String getTable() {
    return this.table;
  }

  @Override
  public byte [] getKey() {
    return this.key;
  }

  public byte [][] getColumns() {
    return this.columns;
  }

  @Override
  public int getPriority() {
    return 1;
  }
}
