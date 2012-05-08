package com.continuuity.fabric.operations.impl;

import com.continuuity.fabric.operations.WriteOperation;

public class OrderedWrite implements WriteOperation {
  private byte [] key;
  private byte [] value;
 
  public OrderedWrite(byte [] key, byte [] value) {
    this.key = key;
    this.value = value;
  }

  public byte [] getKey() {
    return this.key;
  }
 
  public byte [] getValue() {
    return this.value;
  }
}
