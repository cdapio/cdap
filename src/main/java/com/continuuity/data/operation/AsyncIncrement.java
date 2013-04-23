package com.continuuity.data.operation;

/**
 * Atomic asynchronous increment operation.
 *
 * Performs increments of 8 byte (long) keys and columns.  The increment is
 * performed atomically in asynchronous way. Increment result may not be visible before transaction is committed
 * to other operations inside same transaction. Guaranteed to be finished (and visible) when transaction is committed.
 *
 * Supports key-value and columnar operations.
 */
public class AsyncIncrement extends Increment {
//todo: toString()
  public AsyncIncrement(byte[] row, byte[] column, long amount) {
    super(row, column, amount);
  }

  public AsyncIncrement(String table, byte[] row, byte[] column, long amount) {
    super(table, row, column, amount);
  }

  public AsyncIncrement(byte[] row, byte[][] columns, long[] amounts) {
    super(row, columns, amounts);
  }

  public AsyncIncrement(String table, byte[] row, byte[][] columns, long[] amounts) {
    super(table, row, columns, amounts);
  }

  public AsyncIncrement(long id, String table, byte[] row, byte[][] columns, long[] amounts) {
    super(id, table, row, columns, amounts);
  }
}
