package com.continuuity.data.operation;

/**
 * Write asynchronously the value of a key or the values of columns in a row.
 * Write result may not be visible before transaction is committed
 * to other operations inside same transaction. Guaranteed to be finished (and visible) when transaction is committed.
 *
 * Supports both key-value and columnar operations.
 */
public class AsyncWrite extends Write {
//todo: toString()
  public AsyncWrite(byte[] row, byte[] column, byte[] value) {
    super(row, column, value);
  }

  public AsyncWrite(String table, byte[] row, byte[] column, byte[] value) {
    super(table, row, column, value);
  }

  public AsyncWrite(byte[] row, byte[][] columns, byte[][] values) {
    super(row, columns, values);
  }

  public AsyncWrite(String table, byte[] row, byte[][] columns, byte[][] values) {
    super(table, row, columns, values);
  }

  public AsyncWrite(long id, String table, byte[] row, byte[][] columns, byte[][] values) {
    super(id, table, row, columns, values);
  }
}
