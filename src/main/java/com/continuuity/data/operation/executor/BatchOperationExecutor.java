package com.continuuity.data.operation.executor;

import java.util.List;

import com.continuuity.data.operation.type.WriteOperation;


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
   * If no other behavior is defined in implementing class, the default behavior
   * is to perform the specified writes synchronously and in sequence.
   *
   * If an error is reached, execution of subsequent operations is skipped and
   * false is returned.  If all operations are performed successfully, returns
   * true.
   *
   * @see TransactionalOperationExecutor#execute(WriteOperation[])
   *
   * @param writes list of write operations to execute as a batch
   * @return true if success, false if not
   */
  public boolean execute(List<WriteOperation> writes)
  throws BatchOperationException;

}
