package com.continuuity.data.table;

/**
 * An unordered, non-versioned, simple key-value table.
 */
public interface KVTable {

  public void put(byte [] key, byte [] value);

  public byte [] get(byte [] key);
  
  public void delete(byte [] key);
  
  public long increment(byte [] key, long amount);
  
  public boolean compareAndSwap(byte [] key, byte [] expectedValue,
      byte [] newValue);

}
