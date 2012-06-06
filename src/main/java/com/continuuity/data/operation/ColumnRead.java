package com.continuuity.data.operation;

import java.util.Map;

import com.continuuity.data.operation.type.ReadOperation;

public class ColumnRead implements ReadOperation<Map<byte[], byte[]>> {

  private final byte [] key;
  private final byte [][] columns;

  private Map<byte[], byte[]> result;

  public ColumnRead(final byte [] key) {
    this(key, null);
  }

  public ColumnRead(final byte [] key, final byte [][] columns) {
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
