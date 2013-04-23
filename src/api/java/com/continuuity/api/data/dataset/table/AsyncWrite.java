package com.continuuity.api.data.dataset.table;

/**
 * A write to a table. It can write one more columns of a row.
 * <p>
 *   This operation is performed asynchronously, i.e.:
 *   <ul>
 *     <li>
 *       Operation result may not be visible before transaction is committed
 *       to other operations inside same transaction.
 *     </li>
 *     <li>
 *       Operation is guaranteed to be finished (and its result to be visible) when transaction is committed.
 *     </li>
 *   </ul>
 * </p>
 */
public class AsyncWrite extends Write {

  public AsyncWrite(byte[] row, byte[][] columns, byte[][] values) {
    super(row, columns, values);
  }

  public AsyncWrite(byte[] row, byte[] column, byte[] value) {
    super(row, column, value);
  }
}
