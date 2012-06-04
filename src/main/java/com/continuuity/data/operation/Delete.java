package com.continuuity.data.operation;

import com.continuuity.data.operation.type.WriteOperation;

public class Delete implements WriteOperation {

  private final byte [] row;
  private final byte [] column;

  public Delete(byte [] row, byte [] column) {
    this.row = row;
    this.column = column;
  }

  @Override
  public byte [] getKey() {
    return this.row;
  }

  public byte [] getColumn() {
    return this.column;
  }

  @Override
  public int getPriority() {
    return 1;
  }
}
