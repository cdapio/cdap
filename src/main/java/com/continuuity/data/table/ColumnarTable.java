package com.continuuity.data.table;

import java.util.Map;

public interface ColumnarTable {

  public void put(byte [] row, byte [] column, byte [] value);
  
  public void put(byte [] row, byte [][] columns, byte [][] values);
  
  public void delete(byte [] row);
  
  public void delete(byte [] row, byte [] column);

  public Map<byte[], byte[]> get(byte[] row);

  public byte [] get(byte [] row, byte [] column);

  public Map<byte[],byte[]> get(byte [] row, byte [] startColumn,
      byte [] stopColumn);

  public Map<byte[],byte[]> get(byte [] row, byte [][] columns);
  
  public long increment(byte [] row, byte [] column, long amount);
  
  // Conditional Operations
  
  public boolean compareAndSwap(byte [] row, byte [] column,
      byte [] expectedValue, byte [] newValue);

}
