package com.continuuity.api.data.dataset.table;

/**
 * A Delete removes one or more columns from a row. Note that to delete an
 * entire row, the caller needs to know the columns that exist.
 */
public class Delete extends AbstractWriteOperation {
  // the columns to be deleted
  protected byte[][] columns;

  /**
   * Get the columns to delete.
   * @return the column keys of the columns to be deleted
   */
  public byte[][] getColumns() {
    return columns;
  }

  /**
   * Delete several columns.
   * @param row the row key
   * @param columns the column keys of the columns to be deleted
   */
  public Delete(byte[] row, byte[][] columns) {
    super(row);
    this.columns = columns;
  }

  /**
   * Delete a single column.
   * @param row the row key
   * @param column the column key of the column to be deleted
   */
  public Delete(byte[] row, byte[] column) {
    this(row, new byte[][] { column });
  }
}

