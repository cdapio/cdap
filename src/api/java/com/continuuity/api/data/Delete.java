package com.continuuity.api.data;

/**
 * Delete a key or columns.
 */
public class Delete implements WriteOperation {

  /** The key/row */
  private final byte [] key;
  
  /** The columns to be deleted */
  private final byte [][] columns;

  /**
   * Deletes the specified key-value.
   *
   * This is a key-value operation.
   * 
   * @param key the key to delete
   */
  public Delete(final byte [] key) {
    this(key, KV_COL_ARR);
  }

  /**
   * Deletes the specified column in the specified row.
   * 
   * This is a columnar operation.
   * 
   * @param row the row containing the column to delete
   * @param column the column to delete
   */
  public Delete(final byte [] row, final byte [] column) {
    this(row, new byte [][] { column });
  }

  /**
   * Deletes the specified columns in the specified row.
   * 
   * This is a columnar operation.
   * 
   * @param row the row containing the columns to delete
   * @param columns the columns to delete
   */
  public Delete(final byte [] row, final byte [][] columns) {
    this.key = row;
    this.columns = columns;
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
