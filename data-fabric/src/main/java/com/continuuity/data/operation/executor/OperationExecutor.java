package com.continuuity.data.operation.executor;


import com.continuuity.api.data.OperationException;
import java.util.Collection;

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
public interface OperationExecutor {

  // temporary TxDs2 stuff
  public com.continuuity.data2.transaction.Transaction startShort() throws OperationException;
  public com.continuuity.data2.transaction.Transaction startShort(int timeout) throws OperationException;
  public com.continuuity.data2.transaction.Transaction startLong() throws OperationException;

  public boolean canCommit(com.continuuity.data2.transaction.Transaction tx, Collection<byte[]> changeIds)
    throws OperationException;

  public boolean commit(com.continuuity.data2.transaction.Transaction tx) throws OperationException;
  public void abort(com.continuuity.data2.transaction.Transaction tx) throws OperationException;
  public void invalidate(com.continuuity.data2.transaction.Transaction tx) throws OperationException;
}
