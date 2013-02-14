/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.operation.executor.omid;

import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.table.ReadPointer;

import java.util.List;


/**
 * The transaction oracle is the basis for transactions in the data fabric.
 * <ul>
 *   <li>Every transaction receives a unique transaction id.</li>
 *   <li>Transaction ids are strictly monotonic increasing.</li>
 *   <li>All writes performed by a transaction are made with the transaction id as the timestamp.
 *     That is, a transaction that starts later overwrites values written by transactions that
 *     started earlier. </li>
 *   <li>When a transaction begins, it takes a snapshot of the data that includes only transactions
 *     that are committed at that time. That is, transactions do not see the writes of in-flight
 *     transactions. This is implemented as a ReadPointer, which filters out the in-flight writes.</li>
 *   <li>If two overlapping transactions write the same row, then we have a write conflict. We detect
 *     the conflict by comparing, at commit time, the row sets of all overlapping transactions, that is,
 *     transactions that committed during the life time of the current transaction. In case of a conflict,
 *     the later transaction is rolled back.</li>
 *   <li>Roll-back happens by undoing all writes of the transaction. Thus we must remember for every
 *     transaction what needs to be undone. If the rollback fails (unlikely but possible), the transaction's
 *     writes are excluded from future reads.</li>
 * </ul>
 * This is also known as Snapshot Isolation, a variant of Optimistic Concurrency Control.
 *
 * The Oracle is responsible for:
 * <ul>
 *   <li>Assigning transaction ids.</li>
 *   <li>Keeping track of all in-progress transactions and what needs to be undone in case of rollback.</li>
 *   <li>Keeping track of all transactions that need to be ecluded from reads.</li>
 *   <li><Detecting write conflicts.</li>
 * </ul>
 * The following is the contract for how the transaction oracle must be called:
 * <ol>
 *   <li>Start a transaction.</li>
 *   <li>Repeatedly add operations to the transaction. This must be done for all write operations
 *     performed in the transaction. Otherwise rollback of a failed transaction is incomplete.</li>
 *   <li>Either commit or abort the transaction. If commit is successful, done.</li>
 *   <li>Perform all the undo operations returned by the commit or abort. This is important, because
 *     the oracle cannot undo any writes in the data fabric, and it is up to the client (the opex) to
 *     perform the rollback. If this fails, or is skipped for any reason, then step 5 must not be
 *     performed. Otherwise the failed transaction's writes will become visible.</li>
 *   <li>Remove the transaction. This must be called after rollback is complete. Otherwise the set
 *     of invalid transaction (and hance the set of excludes) will grow over time beyond manageability.</li>
 * </ol>
 */
public interface TransactionOracle {

  /**
   * Get a read pointer relative to the current time. This is used by operations/transaction
   * that do not write, hence there is no need to start a full transaction.
   * @return A read pointer that includes all committed transactions.
   */
  public ReadPointer getReadPointer();

  /**
   * Start a new transaction. This assigns a new transaction id and generates a read pointer
   * that excludes all transaction that start in the future, and also excludes all uncommitted
   * transactions at this time. It does, however, include the transaction itself, such that
   * transactions can see their own writes.
   * @return a pair of the read pointer and the new transaction id.
   */
  public Transaction startTransaction();

  /**
   * Add a batch of operations to a transaction. The list of undo operations is saved for that
   * transaction.
   * @param txid the transction id
   * @param undos the list of undo operations required to undo this batch
   * @throws OmidTransactionException if the transaction is not in progress
   */
  public void addToTransaction(long txid, List<Undo> undos) throws OmidTransactionException;

  /**
   * Commit a transaction. This detects write conflicts and possibly aborts the transaction. In
   * that case it returns a list of undo operations to perform the rollback. Otherwise, the
   * transaction is removed from the exclude list and future transactions can see its writes.
   * @param txid the transction id
   * @return a result that indicates whether rollback is needed (if isSuccess() is false), and
   *   if so, a list of undo operations to perform the rollback.
   * @throws OmidTransactionException if the transaction is not in progress
   */
  public TransactionResult commitTransaction(long txid) throws OmidTransactionException;

  /**
   * Abort a running transaction. Returns a list of undo operations to perform rollback.
   * @param txid the transction id
   * @return A result indicating failure (i.e., rollback needed) and the list of undo
   *  operations to perform the rollback
   * @throws OmidTransactionException if the transaction is not in progress
   */
  public TransactionResult abortTransaction(long txid) throws OmidTransactionException;

  /**
   * Remove a transaction from the read excludes. This should be called after a transaction
   * was aborted and the rollback has completed.
   * @param txid the transction id
   * @throws OmidTransactionException if the transaction is not in the list of excludes,
   *   or if the transaction is still in progress.
   */
  void removeTransaction(long txid) throws OmidTransactionException;
}
