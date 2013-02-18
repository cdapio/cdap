package com.continuuity.data.table.converter;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.table.VersionedColumnarTable;
import com.continuuity.data.table.VersionedKVTable;

public class VersionedKVOnVersionedColumnarTable implements VersionedKVTable {

  private static final byte[] COLUMN = new byte[] { (byte) 0 };

  private final VersionedColumnarTable table;

  public VersionedKVOnVersionedColumnarTable(VersionedColumnarTable table) {
    this.table = table;
  }

  @Override
  public void put(byte[] key, byte[] value, long version)
      throws OperationException {
    this.table.put(key, COLUMN, version, value);
  }

  @Override
  public OperationResult<byte[]> get(byte[] key, ReadPointer readPointer)
      throws OperationException {
    return this.table.get(key, COLUMN, readPointer);
  }

  @Override
  public void delete(byte[] key, long version) throws OperationException {
    this.table.delete(key, COLUMN, version);
  }

  @Override
  public long increment(byte[] key, long amount, ReadPointer readPointer,
      long writeVersion) throws OperationException {
    return this.table.increment(key, COLUMN, amount, readPointer, writeVersion);
  }

  @Override
  public void compareAndSwap(byte[] key,
                             byte[] expectedValue, byte[] newValue,
                             ReadPointer readPointer, long writeVersion)
      throws OperationException {
    this.table.compareAndSwap(key, COLUMN, expectedValue, newValue,
        readPointer, writeVersion);
  }

}
