package com.continuuity.data.table;

import java.util.Map;

import com.continuuity.common.utils.ImmutablePair;

public interface VersionedColumnarTable {

  public void put(byte [] row, byte [] column, long version, byte [] value);

  public void put(byte [] row, byte [][] columns, long version,
      byte [][] values);

  public void delete(byte [] row, byte [] column, long version);

  public Map<byte [], byte []> get(byte [] row, ReadPointer readPointer);

  public byte [] get(byte [] row, byte [] column, ReadPointer readPointer);

  public ImmutablePair<byte[],Long> getWithVersion(byte [] row, byte [] column,
      ReadPointer readPointer);

  public Map<byte [], byte []> get(byte [] row, byte [] startColumn,
      byte [] stopColumn, ReadPointer readPointer);

  public Map<byte [], byte []> get(byte [] row, byte [][] columns,
      ReadPointer readPointer);

  public long increment(byte [] row, byte [] column, long amount,
      ReadPointer readPointer, long writeVersion);

  public boolean compareAndSwap(byte [] row, byte [] column,
      byte [] expectedValue, byte [] newValue, ReadPointer readPointer,
      long writeVersion);
}
