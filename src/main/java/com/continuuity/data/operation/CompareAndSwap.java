package com.continuuity.data.operation;

import com.continuuity.data.operation.type.ConditionalWriteOperation;


public class CompareAndSwap implements ConditionalWriteOperation {

  private byte [] key;
  private byte [] expectedValue;
  private byte [] newValue;
 
  public CompareAndSwap(byte [] key, byte [] expectedValue, byte [] newValue) {
    this.key = key;
    this.expectedValue = expectedValue;
    this.newValue = newValue;
  }

  @Override
  public byte [] getKey() {
    return this.key;
  }
 
  public byte [] getExpectedValue() {
    return this.expectedValue;
  }
 
  public byte [] getNewValue() {
    return this.newValue;
  }
}
