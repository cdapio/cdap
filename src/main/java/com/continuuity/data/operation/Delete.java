package com.continuuity.data.operation;

import com.continuuity.data.operation.type.WriteOperation;

public class Delete implements WriteOperation {

  private byte [] row;
  private byte [] column;
 
  public Delete(byte [] row) {
    this(row, null);
  }
  
  public Delete(byte [] row, byte [] column) {
    this.row = row;
    this.column = column;
  }

  public byte [] getKey() {
    return this.row;
  }
 
  public boolean hasColumn() {
    return getColumn() != null;
  }
  
  public byte [] getColumn() {
    return this.column;
  }
}
