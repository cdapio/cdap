package com.continuuity.data.operation.executor.omid;

/**
 * Represents a delete on a table that needs to be undone for transaction rollback.
 */
public class UndoDelete extends UndoWrite {

  public UndoDelete(String table, byte[] key, byte[][] columns) {
    super(table, key, columns);
  }
}
