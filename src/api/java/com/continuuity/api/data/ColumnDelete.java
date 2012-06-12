package com.continuuity.api.data;


public class ColumnDelete implements WriteOperation {

  private final byte [] row;
  private final byte [] column;

  public ColumnDelete(final byte [] row, final byte [] column) {
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
