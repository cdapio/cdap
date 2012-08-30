package com.continuuity.data.operation.executor;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.WriteOperation;

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
   * If no other behavior is defined in implementing class, the default behavior
   * is to perform the specified writes synchronously and in sequence.
   *
   * If an error is reached, execution of subsequent operations is skipped and
   * an exception is thrown.
   *
   * @see TransactionalOperationExecutor#execute
   *
   * @param writes list of write operations to execute as a batch
   * @throws OperationException if anything goes wrong
   */
  public void execute(List<WriteOperation> writes)
      throws OperationException;
}
