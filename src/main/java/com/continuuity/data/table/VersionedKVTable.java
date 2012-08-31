package com.continuuity.data.table;


import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;

public interface VersionedKVTable {

  public void put(byte[] key, byte[] value, long version);

  public OperationResult<byte[]> get(byte[] key, ReadPointer readPointer);

  public void delete(byte[] key, long version);

  public long increment(byte[] key, long amount, ReadPointer readPointer,
      long writeVersion) throws OperationException;

  public void compareAndSwap(byte[] key, byte[] expectedValue,
                             byte[] newValue, ReadPointer readPointer, long writeVersion) throws OperationException;

}
