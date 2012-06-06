package com.continuuity.data.operation;

import com.continuuity.data.operation.type.WriteOperation;

public class OrderedWrite implements WriteOperation {
  private byte [] key;
  private byte [] value;
 
  public OrderedWrite(final byte [] key, final byte [] value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public byte [] getKey() {
    return this.key;
  }
 
  public byte [] getValue() {
    return this.value;
  }

  @Override
  public int getPriority() {
    return 1;
  }
}
