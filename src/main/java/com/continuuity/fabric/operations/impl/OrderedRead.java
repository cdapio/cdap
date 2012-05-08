package com.continuuity.fabric.operations.impl;

import com.continuuity.fabric.operations.ReadOperation;

public class OrderedRead implements ReadOperation<byte[]> {

  private final byte [] startKey;
  private final byte [] endKey;
  private final int limit;

  public OrderedRead(byte [] key) {
    this(key, null, 1);
  }

  public OrderedRead(byte [] startKey, byte [] endKey) {
    this(startKey, endKey, Integer.MAX_VALUE);
  }

  public OrderedRead(byte [] startKey, int limit) {
    this(startKey, null, limit);
  }

  public OrderedRead(byte [] startKey, byte [] endKey, int limit) {
    this.startKey = startKey;
    this.endKey = endKey;
    this.limit = limit;
  }

  public byte [] getStartKey() {
    return this.startKey;
  }

  public byte [] getEndKey() {
    return this.endKey;
  }

  public int getLimit() {
    return this.limit;
  }
  @Override
  public byte[] getResult() {
    // TODO Auto-generated method stub
    return null;
  }

}
