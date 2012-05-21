package com.continuuity.data.operation;

import java.util.Map;

import com.continuuity.data.operation.type.ReadOperation;

public class OrderedRead implements ReadOperation<Map<byte[], byte[]>> {

  private final byte [] startKey;
  private final byte [] endKey;
  private final int limit;

  private Map<byte[], byte[]> result;

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
  public Map<byte[], byte[]> getResult() {
    return this.result;
  }
  
  @Override
  public void setResult(Map<byte[], byte[]> result) {
    this.result = result;
  }

}
