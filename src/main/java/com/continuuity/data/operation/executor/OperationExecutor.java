package com.continuuity.data.operation.executor;


import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.WriteOperation;

import java.util.List;

/**
 * TODO: Write some docs
 *
 * Executes read and write operations.
 *
 * Writes return true or false as to whether they succeeded or not.
 */
public interface OperationExecutor
  extends ReadOperationExecutor, InternalOperationExecutor {

  /**
   * Performs a {@link com.continuuity.data.operation.Write} operation.
   * @param write the operation
   * @throws com.continuuity.api.data.OperationException if execution failed
   */
  public void execute(OperationContext context,
                      WriteOperation write) throws OperationException;

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
   * @param writes list of write operations to execute as a batch
   * @throws OperationException if anything goes wrong
   */
  public void execute(OperationContext context,
                      List<WriteOperation> writes) throws OperationException;


  /**
   * Submit a batch of operations for execution in a transaction. If the transaction is
   * passed in, it is used, otherwise a new transaction is started.
   * @param context the operation context
   * @param transaction the existing transaction, or null to start a new one
   * @param writes the operations to execute
   * @return the transaction (either provided or newly started)
   * @throws OperationException if anything goes wrong
   */
  /*
  public Transaction submit(OperationContext context,
                            Transaction transaction,
                            List<WriteOperation> writes)
    throws OperationException;

  /**
   * Commit an existing transaction
   * @param context the operation context
   * @param transaction the transaction to be committed
   * @throws OperationException if the commit fails for any reason
   */
  /*
  public void commit(OperationContext context,
                     Transaction transaction)
    throws OperationException;

  /**
   * @return the name of the executor, set by the implementation for verbose messages
   */
  public String getName();
}
