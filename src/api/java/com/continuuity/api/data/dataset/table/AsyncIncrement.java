package com.continuuity.api.data.dataset.table;

/**
 * The operation has same affect as {@link Increment}, but is performed asynchronously (see details below).
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
public class AsyncIncrement extends Increment {

  public AsyncIncrement(byte[] row, byte[][] columns, long[] values) {
    super(row, columns, values);
  }

  public AsyncIncrement(byte[] row, byte[] column, long value) {
    super(row, column, value);
  }
}
