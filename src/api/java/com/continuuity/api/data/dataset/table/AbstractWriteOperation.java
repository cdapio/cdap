package com.continuuity.api.data.dataset.table;

public abstract class AbstractWriteOperation implements WriteOperation {

  protected byte[] row;

  /** Constructor from the row key */
  public AbstractWriteOperation(byte[] row) {
    this.row = row;
  }

  /** get the row key of the row to write */
  public byte[] getRow() {
    return this.row;
  }
}
