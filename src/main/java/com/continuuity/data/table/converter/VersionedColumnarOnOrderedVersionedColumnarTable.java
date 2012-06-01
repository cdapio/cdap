package com.continuuity.data.table.converter;

import java.util.Map;

import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.engine.ReadPointer;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.VersionedColumnarTable;
import com.continuuity.data.util.KeyMapper;

public class VersionedColumnarOnOrderedVersionedColumnarTable implements
    VersionedColumnarTable {

  private final OrderedVersionedColumnarTable table;
  
  private final KeyMapper mapper;

  public VersionedColumnarOnOrderedVersionedColumnarTable(
      OrderedVersionedColumnarTable table, KeyMapper mapper) {
    this.table = table;
    this.mapper = mapper;
  }

  @Override
  public void put(byte[] row, byte[] column, long version, byte[] value) {
    table.put(mapper.map(row), column, version, value);
  }

  @Override
  public void put(byte[] row, byte[][] columns, long version, byte[][] values) {
    table.put(mapper.map(row), columns, version, values);
  }

  @Override
  public void delete(byte[] row, long version) {
    table.delete(mapper.map(row), version);
  }

  @Override
  public void delete(byte[] row, byte[] column, long version) {
    table.delete(mapper.map(row), column, version);
  }

  @Override
  public Map<byte[], byte[]> get(byte[] row, ReadPointer readPointer) {
    return table.get(mapper.map(row), readPointer);
  }

  @Override
  public byte[] get(byte[] row, byte[] column, ReadPointer readPointer) {
    return table.get(mapper.map(row), column, readPointer);
  }

  @Override
  public ImmutablePair<byte[], Long> getWithVersion(byte[] row, byte[] column,
      ReadPointer readPointer) {
    return table.getWithVersion(mapper.map(row), column, readPointer);
  }

  @Override
  public Map<byte[], byte[]> get(byte[] row, byte[] startColumn,
      byte[] stopColumn, ReadPointer readPointer) {
    return table.get(mapper.map(row), startColumn, stopColumn, readPointer);
  }

  @Override
  public Map<byte[], byte[]> get(byte[] row, byte[][] columns,
      ReadPointer readPointer) {
    return table.get(mapper.map(row), columns, readPointer);
  }

  @Override
  public long increment(byte[] row, byte[] column, long amount,
      ReadPointer readPointer, long writeVersion) {
    return table.increment(mapper.map(row), column, amount, readPointer,
        writeVersion);
  }

  @Override
  public boolean compareAndSwap(byte[] row, byte[] column,
      byte[] expectedValue, byte[] newValue, ReadPointer readPointer,
      long writeVersion) {
    return table.compareAndSwap(mapper.map(row), column, expectedValue,
        newValue, readPointer, writeVersion);
  }

}
