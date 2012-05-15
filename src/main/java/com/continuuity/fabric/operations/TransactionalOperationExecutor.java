package com.continuuity.fabric.operations;

import java.util.List;

public interface TransactionalOperationExecutor extends OperationExecutor {

  /**
   * Performs the specified writes synchronously and transactionally (the batch
   * appears atomic to readers and the entire batch either completely succeeds
   * or completely fails).
   *
   * Operations may be re-ordered and retryable operations may be automatically
   * retried.
   * 
   * If the batch cannot be completed successfully, false is returned.  In this
   * case, the operations should be re-generated rather than just re-submitted
   * as retryable operations will already have been retried.
   *
   * If the batch is completed successful, method returns truel
   *
   * @param writes write operations to be performed in a transaction
   * @return true if all operations succeeded, false if transaction failed
   */
  @Override
  public boolean execute(List<WriteOperation> writes);

}
