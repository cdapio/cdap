package com.continuuity.data.operation.executor.omid;

import com.continuuity.data.operation.Delete;

/**
 * Represents an operation on a table - other than a delete - that needs to
 * be undone for transaction rollback. It turns out that all these write
 * operations can be undone by a delete.
 */
public class UndoWrite extends Delete implements Undo {

  public UndoWrite(String table, byte[] key, byte[][] columns) {
    super(table, key, columns);
  }

  @Override
  public RowSet.Row getRow() {
    return new RowSet.Row(this.getTable(), this.getKey());
  }
}
