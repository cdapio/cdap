package com.continuuity.data.table;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;

/**
 * An unordered, non-versioned, simple key-value table.
 */
public interface KVTable {

  public void put(byte [] key, byte [] value);

  public OperationResult<byte[]> get(byte[] key);
  
  public void delete(byte [] key);
  
  public long increment(byte [] key, long amount) throws OperationException;
  
  public void compareAndSwap(byte[] key, byte[] expectedValue,
                             byte[] newValue) throws OperationException;

}
