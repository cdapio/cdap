package com.continuuity.fabric.operations.impl;

import java.util.Map;

import com.continuuity.fabric.operations.ReadOperation;

public class ColumnRead implements ReadOperation<Map<byte[], byte[]>> {

  private final byte [] key;
  private final byte [][] columns;

  private Map<byte[], byte[]> result;

  public ColumnRead(byte [] key) {
    this(key, null);
  }

  public ColumnRead(byte [] key, byte [][] columns) {
    this.key = key;
    this.columns = columns;
  }

  public byte [] getKey() {
    return this.key;
  }

  public byte [][] getColumns() {
    return this.columns;
  }

  @Override
  public Map<byte[], byte[]> getResult() {
    return this.result;
  }
  
  @Override
  public void setResult(Map<byte[], byte[]> result) {
    this.result = result;
  }

}
