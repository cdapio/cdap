package com.continuuity.data.table;

import com.continuuity.data.engine.ReadPointer;

public interface VersionedKVTable {

  public void put(byte[] key, byte[] value, long version);

  public byte[] get(byte[] key, ReadPointer readPointer);

  public void delete(byte[] key, long version);

  public long increment(byte[] key, long amount, ReadPointer readPointer,
      long writeVersion);

  public boolean compareAndSwap(byte[] key, byte[] expectedValue,
      byte[] newValue, ReadPointer readPointer, long writeVersion);

}
