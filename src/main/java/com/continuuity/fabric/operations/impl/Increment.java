package com.continuuity.fabric.operations.impl;

import com.continuuity.fabric.operations.WriteOperation;

public class Increment implements WriteOperation {

  private byte [] key;
  private long amount;
 
  public Increment(byte [] key, long amount) {
    this.key = key;
    this.amount = amount;
  }

  public byte [] getKey() {
    return this.key;
  }
 
  public long getAmount() {
    return this.amount;
  }
}
