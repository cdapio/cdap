package com.continuuity.data.table;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;

import java.util.Map;

public interface ColumnarTable {

  public void put(byte [] row, byte [] column, byte [] value);
  
  public void put(byte [] row, byte [][] columns, byte [][] values);
  
  public void delete(byte [] row, byte [] column);

  public OperationResult<Map<byte[], byte[]>> get(byte[] row);

  public OperationResult<byte[]> get(byte [] row, byte [] column);

  public OperationResult<Map<byte[], byte[]>> get(byte[] row, byte[] startColumn,
                                                  byte[] stopColumn);

  public OperationResult<Map<byte[], byte[]>> get(byte[] row, byte[][] columns);
  
  public long increment(byte [] row, byte [] column, long amount) throws OperationException;
  
  // Conditional Operations
  
  public void compareAndSwap(byte [] row, byte [] column,
      byte [] expectedValue, byte [] newValue) throws OperationException;

}
