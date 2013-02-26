package com.continuuity.data.operation;

public class OrderedWrite extends WriteOperation {
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

  @Override
  public int getSize() {
    return 0;
  }
}
