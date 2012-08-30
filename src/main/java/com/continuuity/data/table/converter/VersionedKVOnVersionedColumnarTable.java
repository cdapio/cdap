package com.continuuity.data.table.converter;

import com.continuuity.data.table.ReadPointer;
import com.continuuity.data.table.VersionedColumnarTable;
import com.continuuity.data.table.VersionedKVTable;

public class VersionedKVOnVersionedColumnarTable implements VersionedKVTable {

  private static final byte[] COLUMN = new byte[] { (byte) 0 };

  private final VersionedColumnarTable table;

  public VersionedKVOnVersionedColumnarTable(VersionedColumnarTable table) {
    this.table = table;
  }

  @Override
  public void put(byte[] key, byte[] value, long version) {
    this.table.put(key, COLUMN, version, value);
  }

  @Override
  public byte[] get(byte[] key, ReadPointer readPointer) {
    return this.table.get(key, COLUMN, readPointer);
  }

  @Override
  public void delete(byte[] key, long version) {
    this.table.delete(key, COLUMN, version);
  }

  @Override
  public long increment(byte[] key, long amount, ReadPointer readPointer,
      long writeVersion) {
    return this.table.increment(key, COLUMN, amount, readPointer, writeVersion);
  }

  @Override
  public boolean compareAndSwap(byte[] key, byte[] expectedValue,
      byte[] newValue, ReadPointer readPointer, long writeVersion) {
    try {
      return this.table.compareAndSwap(key, COLUMN, expectedValue, newValue,
          readPointer, writeVersion);
    } catch (com.continuuity.api.data.OperationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

}
