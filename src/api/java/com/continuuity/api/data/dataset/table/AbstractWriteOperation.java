package com.continuuity.api.data.dataset.table;

/**
 * Base class for all write operations.
 */
public abstract class AbstractWriteOperation implements WriteOperation {

  // the row key of the operation
  protected byte[] row;

  /**
   * Constructor from the row key.
   * @param row the row key
   */
  public AbstractWriteOperation(byte[] row) {
    this.row = row;
  }

  /**
   * Get the row key of the row to write.
   * @return the row key
   */
  public byte[] getRow() {
    return this.row;
  }
}
