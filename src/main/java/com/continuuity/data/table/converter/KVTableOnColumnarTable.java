package com.continuuity.data.table.converter;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.table.ColumnarTable;
import com.continuuity.data.table.KVTable;

public class KVTableOnColumnarTable implements KVTable {

  private static final byte [] COLUMN = new byte [] { (byte)0 };

  private final ColumnarTable columnarTable;
  
  public KVTableOnColumnarTable(ColumnarTable columnarTable) {
    this.columnarTable = columnarTable;
  }
  
  @Override
  public void put(byte[] key, byte[] value) throws OperationException {
    columnarTable.put(key, COLUMN, value);
  }

  @Override
  public OperationResult<byte[]> get(byte[] key) throws OperationException {
    return columnarTable.get(key, COLUMN);
  }

  @Override
  public void delete(byte[] key) throws OperationException {
    columnarTable.delete(key, COLUMN);
  }

  @Override
  public long increment(byte[] key, long amount) throws OperationException {
    return columnarTable.increment(key, COLUMN, amount);
  }

  @Override
  public void compareAndSwap(byte[] key, byte[] expectedValue,
                             byte[] newValue) throws OperationException {
    columnarTable.compareAndSwap(key, COLUMN, expectedValue, newValue);
  }

}
