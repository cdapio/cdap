package com.continuuity.data.operation.executor;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.WriteOperation;

import java.util.List;

/**
 * An executor that can execute a batch of operations as a transaction.
 */
public interface TransactionalOperationExecutor extends OperationExecutor {

  /**
   * Performs the specified writes synchronously and transactionally (the batch
   * appears atomic to readers and the entire batch either completely succeeds
   * or completely fails).
   *
   * Operations may be re-ordered and retriable operations may be automatically
   * retried.
   * 
   * If the batch cannot be completed successfully, an exception is thrown.
   * In this case, the operations should be re-generated rather than just
   * re-submitted as retriable operations will already have been retried.
   *
   *
   * @param writes write operations to be performed in a transaction
   * @throws OperationException
   */
  @Override
  public void commit(OperationContext context, List<WriteOperation> writes)
      throws OperationException;

}
