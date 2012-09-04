package com.continuuity.data.table.converter;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.table.ColumnarTable;
import com.continuuity.data.table.SimpleReadPointer;
import com.continuuity.data.table.VersionedColumnarTable;

import java.util.Map;

public class ColumnarOnVersionedColumnarTable implements ColumnarTable {

  private final VersionedColumnarTable table;

  private final TimestampOracle oracle;

  public ColumnarOnVersionedColumnarTable(VersionedColumnarTable table,
      TimestampOracle timestampOracle) {
    this.table = table;
    this.oracle = timestampOracle;
  }

  @Override
  public void put(byte[] row, byte[] column, byte[] value)
      throws OperationException {
    this.table.put(row, column, this.oracle.getTimestamp(), value);
  }

  @Override
  public void put(byte[] row, byte[][] columns, byte[][] values)
      throws OperationException {
    this.table.put(row, columns, this.oracle.getTimestamp(), values);
  }

  @Override
  public void delete(byte[] row, byte[] column) throws OperationException {
    this.table.delete(row, column, this.oracle.getTimestamp());
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> get(byte[] row)
      throws OperationException {
    return this.table.get(row,
        new SimpleReadPointer(this.oracle.getTimestamp()));
  }

  @Override
  public OperationResult<byte[]> get(byte[] row, byte[] column)
      throws OperationException {
    return this.table.get(row, column,
        new SimpleReadPointer(this.oracle.getTimestamp()));
  }

  @Override
  public OperationResult<Map<byte[], byte[]>>
  get(byte[] row, byte[] startColumn, byte[] stopColumn)
      throws OperationException {
    return this.table.get(row, startColumn, stopColumn,
        new SimpleReadPointer(this.oracle.getTimestamp()));
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> get(byte[] row, byte[][] columns)
      throws OperationException {
    return this.table.get(row, columns,
        new SimpleReadPointer(this.oracle.getTimestamp()));
  }

  @Override
  public long increment(byte[] row, byte[] column, long amount) throws OperationException {
    long now = this.oracle.getTimestamp();
    return this.table.increment(row, column, amount,
        new SimpleReadPointer(now), now);
  }

  @Override
  public void compareAndSwap(byte[] row, byte[] column,
                             byte[] expectedValue, byte[] newValue)
      throws OperationException {

    long now = this.oracle.getTimestamp();
    this.table.compareAndSwap(row, column, expectedValue, newValue,
        new SimpleReadPointer(now), now);
  }

}
