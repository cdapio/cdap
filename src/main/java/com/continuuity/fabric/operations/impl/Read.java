package com.continuuity.fabric.operations.impl;

import com.continuuity.fabric.operations.ReadOperation;

public class Read implements ReadOperation<byte[]> {

  private final byte [] key;

  public Read(byte [] key) {
    this.key = key;
  }

  public byte [] getKey() {
    return this.key;
  }

  public byte [] getResult() {
    return null;
  }

  @Override
  public void setResult(byte[] t) {
    // TODO Auto-generated method stub
    
  }
}
