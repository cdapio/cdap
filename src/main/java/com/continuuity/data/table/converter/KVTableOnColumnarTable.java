package com.continuuity.data.table.converter;

import com.continuuity.data.table.ColumnarTable;
import com.continuuity.data.table.KVTable;

public class KVTableOnColumnarTable implements KVTable {

  private static final byte [] COLUMN = new byte [] { (byte)0 };

  private final ColumnarTable columnarTable;
  
  public KVTableOnColumnarTable(ColumnarTable columnarTable) {
    this.columnarTable = columnarTable;
  }
  
  @Override
  public void put(byte[] key, byte[] value) {
    columnarTable.put(key, COLUMN, value);
  }

  @Override
  public byte[] get(byte[] key) {
    return columnarTable.get(key, COLUMN);
  }

  @Override
  public void delete(byte[] key) {
    columnarTable.delete(key, COLUMN);
  }

  @Override
  public long increment(byte[] key, long amount) {
    return columnarTable.increment(key, COLUMN, amount);
  }

  @Override
  public boolean compareAndSwap(byte[] key, byte[] expectedValue,
      byte[] newValue) {
    return columnarTable.compareAndSwap(key, COLUMN, expectedValue, newValue);
  }

}
