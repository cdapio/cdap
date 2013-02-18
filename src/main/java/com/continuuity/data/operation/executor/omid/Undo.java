package com.continuuity.data.operation.executor.omid;

/**
 * This is a base interface for representing an operaton that needs to
 * be undone in case of transaction rollback. It has one method to get
 * the row key of that operation. This is needed by the oracle to extract
 * the row set of a transaction - assuming that the row key of the undo is
 * the same as the row key of the operation itself.
 */
public interface Undo {

  /**
   * @return the row key of the operation that needs to be recorded. If this
   *         returns null, that means that the operation is excluded from
   *         conflict detection (for instance, a queue operation).
   */
  public RowSet.Row getRow();

}
