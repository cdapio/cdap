package com.continuuity.fabric.operations.impl;

import com.continuuity.fabric.operations.ConditionalWriteOperation;


public class CompareAndSwap implements ConditionalWriteOperation {

  private byte [] key;
  private byte [] expectedValue;
  private byte [] newValue;
 
  public CompareAndSwap(byte [] key, byte [] expectedValue, byte [] newValue) {
    this.key = key;
    this.expectedValue = expectedValue;
    this.newValue = newValue;
  }

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
