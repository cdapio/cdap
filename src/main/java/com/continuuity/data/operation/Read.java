package com.continuuity.data.operation;

import com.continuuity.data.operation.type.ReadOperation;

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
