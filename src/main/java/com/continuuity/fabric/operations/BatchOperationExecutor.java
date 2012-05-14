package com.continuuity.fabric.operations;

import java.util.List;


public interface BatchOperationExecutor {

  /**
   * Executes the specified list of write operations as a batch.
   *
   * Returns true if successful, false if not.
   * 
   * The semantics of how this is actually executed is defined within each
   * implementation.  In all cases, it is possible that the list of operations
   * is re-ordered.
   * 
   * @see SimpleOperationExecutor#execute(WriteOperation[])
   * @see TransactionalOperationExecutor#execute(WriteOperation[])
   * 
   * @param writes list of write operations to execute as a batch
   * @return true if success, false if not
   */
  public boolean execute(List<WriteOperation> writes);

}
