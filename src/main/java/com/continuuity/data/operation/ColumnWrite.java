package com.continuuity.data.operation;

import com.continuuity.data.operation.type.WriteOperation;

public class ColumnWrite implements WriteOperation {
  private final byte [] key;
  private final byte [][] columns;
  private final byte [][] values;
 
  public ColumnWrite(byte [] key, byte [][] columns, byte [][] values) {
    this.key = key;
    this.columns = columns;
    this.values = values;
  }

  @Override
  public byte [] getKey() {
    return this.key;
  }
 
  public byte [][] getColumns() {
    return this.columns;
  }
 
  public byte [][] getValues() {
    return this.values;
  }

  @Override
  public int getPriority() {
    return 1;
  }
}
