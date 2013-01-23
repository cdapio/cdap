package com.continuuity.api.data.dataset.table;

/**
 * common interface for all write operations
 */
public abstract class WriteOperation {

  protected byte[] row;

  /** Constructor from the row key */
  public WriteOperation(byte[] row) {
    this.row = row;
  }

  /** get the row key of the row to write */
  public byte[] getRow() {
    return this.row;
  }
}
