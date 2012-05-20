package com.continuuity.fabric.operations.impl;

import com.continuuity.fabric.operations.WriteOperation;

public class ColumnWrite implements WriteOperation {
  private final byte [] key;
  private final byte [][] columns;
  private final byte [][] values;
 
  public ColumnWrite(byte [] key, byte [][] columns, byte [][] values) {
    this.key = key;
    this.columns = columns;
    this.values = values;
  }

  public byte [] getKey() {
    return this.key;
  }
 
  public byte [][] getColumns() {
    return this.columns;
  }
 
  public byte [][] getValues() {
    return this.values;
  }
}
