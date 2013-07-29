package com.continuuity.data.operation.executor;


import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.GetSplits;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.KeyRange;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadAllKeys;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.data.operation.Scan;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.ttqueue.admin.QueueConfigure;
import com.continuuity.data.operation.ttqueue.admin.QueueConfigureGroups;
import com.continuuity.data.operation.ttqueue.admin.QueueDropInflight;
import com.continuuity.data.table.Scanner;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Executes read and write operations.
 *
 * Writes throw an exception if they fail; reads normally succeed (expect in
 * case of system errors) but may return an empty result.
 *
 * The semantics of operation execution is different for each implementation.
 * However, we do assume a concept of transactions. A transaction is a group
 * of operations that are executed together. This can be done in two ways:
 * <ol>
 *   <li>(Anonymous transactions). The client submits a batch of write operations
 *     to be executed as a transaction, but that transaction is never exposed
 *     to the client. The operation executor starts a new transaction, runs
 *     the batch of operations, and commits the transaction.
 *   </li>
 *   <li>(Client-side transactions). The client explicitly starts a transaction
 *     and then repeatedly submit more operations for the transaction. In this
 *     case the contract is that the client must either commit or - in case of
 *     failure - abort the transaction. It is very important that the client
 *     obeys this contract, otherwise the transaction will remain active and
 *     may consume system resources and/or block other operations. Client-side
 *     transactions are useful because they allow execution of read or read/write
 *     operations in the context of the transaction. For instance, one can read
 *     a value from a table, perform some custom computation on the client side
 *     and then store the result with a write operation. That is not possible
 *     with anonymous transactions.
 *   </li>
 * </ol>
 *
 * For all transactions, the operation executor is allowed to re-order the
 * operations in the transaction, as long as it keeps the relative order of
 * dependent operations.
 *
 * If an error is reached during a transaction, the transaction is aborted
 * and an exception is thrown. In this case it is the responsibility of the
 * operation executor to roll back any writes that may have been performed
 * as part of the transaction. The client should not attempt to undo any of
 * its operations.
 *
 * Even though this interface provisions for transactions, some implementations
 * of OperationExecutor may not actually implement transactions, or they may
 * not give the typical ACID guarantees for transactions. If that is the case,
 * the documentation of the executor must clearly state it. Such implementations
 * should be mainly used for testing of special cases etc. but not in production.
 */
public interface OperationExecutor
  extends InternalOperationExecutor {

  /**
   * @return the name of the executor, set by the implementation for verbose messages
   */
  public String getName();

  /**
   * Performs and commits a {@link com.continuuity.data.operation.WriteOperation}
   * in an anonymous transaction.
   * @param write the operation
   * @throws OperationException if execution failed
   */
  public void commit(OperationContext context,
                     WriteOperation write)
    throws OperationException;

  /**
   * Executes the specified list of write operations as an anonymous transaction.
   *
   * @param writes list of write operations to execute
   * @throws OperationException if anything goes wrong
   */
  public void commit(OperationContext context,
                     List<WriteOperation> writes)
    throws OperationException;

  /**
   * Start a client-side transaction.
   * @param trackChanges if false then started transaction doesn't track changes and hence no conflicts with other
   *                     transactions are detected
   * @return the new transaction
   */
  public Transaction startTransaction(OperationContext context, boolean trackChanges)
    throws OperationException;

  /**
   * Submit a batch of operations for execution in a client-side transaction.
   * An existing transaction can be passed in, or otherwise this methods starts
   * a new transaction. If any of the operations fail, the transaction is aborted
   * and an exception is thrown.
   *
   * Note: In case of an executor that involves RPC, passing null for the
   * transaction allows starting the transaction and executing the operations
   * in a single call, thus saves an RPC round-trip.
   *
   * @param context the operation context
   * @param transaction the existing transaction, or null to start a new one
   * @param writes the operations to execute
   * @return the transaction (either the provided one or a newly started one)
   * @throws OperationException if anything goes wrong
   */
  public Transaction execute(OperationContext context,
                             @Nullable Transaction transaction,
                             List<WriteOperation> writes)
    throws OperationException;

  /**
   * Commit a client-side transaction. If the commit fails, the transaction is
   * aborted and an exception is thrown.
   * @param context the operation context
   * @param transaction the transaction to be committed
   * @throws OperationException if the commit fails for any reason
   */
  public void commit(OperationContext context,
                     Transaction transaction)
    throws OperationException;

  /**
   * Execute a batch of write operations in a client-side transaction and commit
   * the transaction. An existing transaction can be passed in, or otherwise this
   * methods starts a new transaction. If any operation or the the commit fails,
   * the transaction is aborted and an exception is thrown.
   *
   * Note: Passing in null for the transaction makes this an anonymous transaction.
   *
   * @param context the operation context
   * @param writes the operations to execute
   * @param transaction the transaction to be committed
   * @throws OperationException if any operation or the commit fails
   */
  public void commit(OperationContext context,
                     @Nullable Transaction transaction,
                     List<WriteOperation> writes)
    throws OperationException;

  /**
   * Abort an existing transaction.
   * @param context the operation context
   * @param transaction the transaction to be committed
   * @throws OperationException if the abort fails for any reason
   */
  public void abort(OperationContext context,
                    Transaction transaction)
    throws OperationException;

  /**
   * Executes a {@link com.continuuity.data.operation.Increment} operation in an
   * anonymous (singleton) transaction.
   *
   * @param context the operation context
   * @param increment the operation
   * @return a map of columns to the new, incremented values.
   * @throws OperationException is something goes wrong
   */
  public Map<byte[], Long> increment(OperationContext context,
                                     Increment increment)
    throws OperationException;

  /**
   * Executes a {@link com.continuuity.data.operation.Increment} operation.
   * A valid transaction must be passed in, and the operation is performed in
   * that client-side transaction.
   *
   * @param context the operation context
   * @param transaction an existing, valid transaction
   * @param increment the operation
   * @return a result object containing a map of columns to the new, incremented
   *         values.
   * @throws OperationException is something goes wrong
   */
  public Map<byte[], Long> increment(OperationContext context,
                                     Transaction transaction,
                                     Increment increment)
    throws OperationException;

  /**
   * Executes a {@link com.continuuity.data.operation.ReadAllKeys} operation
   * in an anonymous (singleton) transaction.
   *
   * @param context the operation context
   * @param readKeys the operation
   * @return a result object containing a list of keys if found. If no
   * keys are found, then the result object will be empty and the status
   * code will be KEY_NOT_FOUND.
   * @throws OperationException is something goes wrong
   */
  public OperationResult<List<byte[]>> execute(OperationContext context,
                                               ReadAllKeys readKeys)
    throws OperationException;

  /**
   * Executes a {@link com.continuuity.data.operation.ReadAllKeys} operation.
   * If a non-null transaction is passed in, the operation is performed in that
   * client-side transaction. Otherwise it is performed as an anonymous
   * transaction.
   *
   * @param context the operation context
   * @param readKeys the operation
   * @param transaction an existing transaction, or null to perform an anonymous
   *                    transaction
   * @return a result object containing a list of keys if none found. If no
   * keys are found, then the result object will be empty and the status
   * code will be KEY_NOT_FOUND.
   * @throws OperationException is something goes wrong
   */
  public OperationResult<List<byte[]>> execute(OperationContext context,
                                               @Nullable Transaction transaction,
                                               ReadAllKeys readKeys)
    throws OperationException;

  /**
   * Executes a {@link com.continuuity.data.operation.GetSplits} operation
   * in an anonymous (singleton) transaction.
   *
   * @param context the operation context
   * @param getSplits the operation
   * @return a result object containing a list of key ranges, each representing one split.
   * If no splits are found for the given criteria, then the result object will be empty and the status
   * code will be KEY_NOT_FOUND.
   * @throws OperationException is something goes wrong
   */
  public OperationResult<List<KeyRange>> execute(OperationContext context,
                                                 GetSplits getSplits)
    throws OperationException;

  /**
   * Executes a {@link com.continuuity.data.operation.GetSplits} operation.
   * If a non-null transaction is passed in, the operation is performed in that
   * client-side transaction. Otherwise it is performed as an anonymous
   * transaction.
   *
   * @param context the operation context
   * @param getSplits the operation
   * @param transaction an existing transaction, or null to perform an anonymous
   *                    transaction
   * @return a result object containing a list of key ranges, each representing one split.
   * If no splits are found for the given criteria, then the result object will be empty and the status
   * code will be KEY_NOT_FOUND.
   * @throws OperationException is something goes wrong
   */
  public OperationResult<List<KeyRange>> execute(OperationContext context,
                                                 @Nullable Transaction transaction,
                                                 GetSplits getSplits)
    throws OperationException;

  /**
   * Executes a {@link com.continuuity.data.operation.Read} operation.
   *
   * @param context the operation context
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
   * Executes a {@link com.continuuity.data.operation.Read} operation. If
   * a non-null transaction is passed in, the operation is performed in that
   * client-side transaction. Otherwise it is performed as an anonymous
   * transaction.
   *
   * @param context the operation context
   * @param transaction an existing transaction, or null to perform an anonymous
   *                    transaction
   * @param read the operation
   * @return a result object containing a map of columns to values if the key
   *    is found. If the key is not found, the result will be empty and the
   *    status code is KEY_NOT_FOUND.
   * @throws OperationException is something goes wrong
   */
  public OperationResult<Map<byte[], byte[]>> execute(OperationContext context,
                                                      @Nullable Transaction transaction,
                                                      Read read)
    throws OperationException;

  /**
   * Executes a {@link com.continuuity.data.operation.ReadColumnRange} operation
   * in an anonymous (singleton) transaction.
   *
   * @param context the operation context
   * @param readColumnRange the operation
   * @return a result object containing a map of columns to values. If the
   * key is not found, the result will be empty and the status code is
   * KEY_NOT_FOUND. If the key exists but there are no columns the given range,
   * then the result is empty with status code COLUMN_NOT_FOUND.
   * @throws OperationException is something goes wrong
   */
  public OperationResult<Map<byte[], byte[]>> execute(OperationContext context,
                                                      ReadColumnRange readColumnRange)
    throws OperationException;

  /**
   * Executes a {@link com.continuuity.data.operation.ReadColumnRange} operation.
   * If a non-null transaction is passed in, the operation is performed in that
   * client-side transaction. Otherwise it is performed as an anonymous
   * transaction.
   *
   * @param context the operation context
   * @param readColumnRange the operation
   * @param transaction an existing transaction, or null to perform an anonymous
   *                    transaction
   * @return a result object containing a map of columns to values. If the
   * key is not found, the result will be empty and the status code is
   * KEY_NOT_FOUND. If the key exists but there are no columns the given range,
   * then the result is empty with status code COLUMN_NOT_FOUND.
   * @throws OperationException is something goes wrong
   */
  public OperationResult<Map<byte[], byte[]>> execute(OperationContext context,
                                                      @Nullable Transaction transaction,
                                                      ReadColumnRange readColumnRange)
    throws OperationException;

  /**
   * Executes a {@link com.continuuity.data.operation.ttqueue.admin.QueueConfigure} operation
   * outside a transaction.
   * @param context the operation context
   * @param configure the QueueConfigure operation to execute
   * @throws OperationException in case of errors
   */
  public void execute(OperationContext context, QueueConfigure configure) throws OperationException;

  /**
   * Executes a {@link com.continuuity.data.operation.ttqueue.admin.QueueConfigureGroups} operation
   * outside a transaction.
   * @param context the operation context
   * @param configure the QueueConfigureGroups operation to execute
   * @throws OperationException
   */
  public void execute(OperationContext context, final QueueConfigureGroups configure) throws OperationException;

  /**
   * Executes a {@link com.continuuity.data.operation.ttqueue.admin.QueueDropInflight} operation outside a transaction.
   * @param context the operation context
   * @param op the QueueDropInflight context
   * @throws OperationException
   */
  public void execute(OperationContext context, final QueueDropInflight op) throws OperationException;

  /**
   * Returns a scanner for the given table. Note: Some implementations may not support this operation. For example,
   * because a scanner has a stateful API, a remote operation executor may not support this (the scanner should
   * connect directly to the data-fabric, not go through a network connection for every call).
   * @param context the operation context
   * @param transaction QueueConfigure is not executed in a transaction, can be null
   * @param scan the Scan operation
   * @throws OperationException in case of errors
   */
  public Scanner scan(OperationContext context, @Nullable Transaction transaction, Scan scan)
    throws OperationException;

  // temporary TxDs2 stuff
  public com.continuuity.data2.transaction.Transaction start() throws OperationException;

  public boolean canCommit(com.continuuity.data2.transaction.Transaction tx, Collection<byte[]> changeIds)
    throws OperationException;

  public boolean commit(com.continuuity.data2.transaction.Transaction tx) throws OperationException;

  public boolean abort(com.continuuity.data2.transaction.Transaction tx) throws OperationException;
}
