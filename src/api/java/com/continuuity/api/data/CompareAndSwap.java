package com.continuuity.api.data;



public class CompareAndSwap implements ConditionalWriteOperation {

  private byte [] key;
  private byte [] expectedValue;
  private byte [] newValue;
 
  public CompareAndSwap(final byte [] key, final byte [] expectedValue,
      final byte [] newValue) {
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

  @Override
  public int getPriority() {
    return 1;
  }
}
