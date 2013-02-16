package com.continuuity.data.operation.executor;


import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadAllKeys;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.data.operation.WriteOperation;

import java.util.List;
import java.util.Map;

/**
 * TODO: Update these docs
 *
 * Executes read and write operations.
 *
 * Writes throw an exception if they fail; reads normally succeed (expect in
 * case of system errors) but may return an empty result.
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
 * It is possible to submit a batch of operations to be executed together. This
 * allows for the implementation to execute them inside a transaction. It is also
 * possible to keep the transaction open after a write and submit more operations
 * later to continue the transaction. Also, a read operation can be executed in
 * the context of the same transaction. It is important that the transaction is
 * properly committed or aborted in that case.
 */
public interface OperationExecutor
  extends InternalOperationExecutor {

  /**
   * @return the name of the executor, set by the implementation for verbose messages
   */
  public String getName();

  /**
   * Performs a {@link com.continuuity.data.operation.Write} operation.
   * @param write the operation
   * @throws com.continuuity.api.data.OperationException if execution failed
   */
  public void execute(OperationContext context,
                      WriteOperation write)
    throws OperationException;

  /**
   * Executes the specified list of write operations as a batch.
   *
   * @param writes list of write operations to execute as a batch
   * @throws OperationException if anything goes wrong
   */
  public void execute(OperationContext context,
                      List<WriteOperation> writes)
    throws OperationException;

  /**
   * Start a transaction
   */
  public Transaction startTransaction(OperationContext context)
    throws OperationException;

  /**
   * Submit a batch of operations for execution in a transaction. If the transaction is
   * passed in, it is used, otherwise a new transaction is started.
   * @param context the operation context
   * @param transaction the existing transaction, or null to start a new one
   * @param writes the operations to execute
   * @return the transaction (either provided or newly started)
   * @throws OperationException if anything goes wrong
   */
  public Transaction execute(OperationContext context,
                             Transaction transaction,
                             List<WriteOperation> writes)
    throws OperationException;

  /**
   * Commit an existing transaction. If the commit fails, the transaction is
   * aborted and an exception is thrown.
   * @param context the operation context
   * @param transaction the transaction to be committed
   * @throws OperationException if the commit fails for any reason
   */
  public void commit(OperationContext context,
                     Transaction transaction)
    throws OperationException;

  /**
   * Execute a batch of write operations in an existing transaction and commit.
   * If the commit fails, the transaction is aborted and an exception is thrown.
   * @param context the operation context
   * @param transaction the transaction to be committed
   * @throws OperationException if the commit fails for any reason
   */
  public void commit(OperationContext context,
                     Transaction transaction,
                     List<WriteOperation> writes)
    throws OperationException;

  /**
   * Abort an existing transaction
   * @param context the operation context
   * @param transaction the transaction to be committed
   * @throws OperationException if the abort fails for any reason
   */
  public void abort(OperationContext context,
                     Transaction transaction)
    throws OperationException;

  /**
   * Executes a {@link com.continuuity.data.operation.Read} operation.
   * @param read the operation
   * @return a result object containing a map of columns to values if the key
   *    is found. If the key is not found, the result will be empty and the
   *    status code is KEY_NOT_FOUND.
   * @throws OperationException is something goes wrong
   */
  public OperationResult<Map<byte[], byte[]>> execute(OperationContext context,
                                                      Read read)
    throws OperationException;

  /**
   * Executes a {@link com.continuuity.data.operation.Read} operation
   * in an existing transaction
   * @param read the operation
   * @param transaction the existing transaction
   * @return a result object containing a map of columns to values if the key
   *    is found. If the key is not found, the result will be empty and the
   *    status code is KEY_NOT_FOUND.
   * @throws OperationException is something goes wrong
   */
  public OperationResult<Map<byte[], byte[]>> execute(OperationContext context,
                                                      Transaction transaction,
                                                      Read read)
    throws OperationException;

  /**
   * Executes a {@link com.continuuity.data.operation.ReadAllKeys} operation.
   * @param readKeys the operation
   * @return a result object containing a list of keys if none found. If no
   * keys are found, then the result object will be empty and the status
   * code will be KEY_NOT_FOUND.
   * @throws OperationException is something goes wrong
   */
  public OperationResult<List<byte[]>> execute(OperationContext context,
                                               ReadAllKeys readKeys)
    throws OperationException;

  /**
   * Executes a {@link com.continuuity.data.operation.ReadAllKeys} operation
   * within an existing transaction.
   * @param readKeys the operation
   * @param transaction the existing transaction
   * @return a result object containing a list of keys if none found. If no
   * keys are found, then the result object will be empty and the status
   * code will be KEY_NOT_FOUND.
   * @throws OperationException is something goes wrong
   */
  public OperationResult<List<byte[]>> execute(OperationContext context,
                                               Transaction transaction,
                                               ReadAllKeys readKeys)
    throws OperationException;

  /**
   * Executes a {@link com.continuuity.data.operation.ReadColumnRange} operation.
   * @param readColumnRange the operation
   * @return a result object containing a map of columns to values. If the
   * key is not found, the result will be empty and the status code is
   * KEY_NOT_FOUND. If the key exists but there are no columns the given range,
   * then the result is empty with status code COLUMN_NOT_FOUND.
   * @throws OperationException is something goes wrong
   */
  public OperationResult<Map<byte[], byte[]>> execute(OperationContext context,
                                                      ReadColumnRange readColumnRange) throws OperationException;

  /**
   * Executes a {@link com.continuuity.data.operation.ReadColumnRange} operation in the
   * context of an existing transaction.
   * @param readColumnRange the operation
   * @param transaction the existing transaction
   * @return a result object containing a map of columns to values. If the
   * key is not found, the result will be empty and the status code is
   * KEY_NOT_FOUND. If the key exists but there are no columns the given range,
   * then the result is empty with status code COLUMN_NOT_FOUND.
   * @throws OperationException is something goes wrong
   */
  public OperationResult<Map<byte[], byte[]>> execute(OperationContext context,
                                                      Transaction transaction,
                                                      ReadColumnRange readColumnRange) throws OperationException;
}
