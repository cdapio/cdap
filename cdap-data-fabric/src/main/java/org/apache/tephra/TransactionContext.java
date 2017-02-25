/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import javax.annotation.Nullable;

/**
 * Utility class that encapsulates the transaction life cycle over a given set of
 * transaction-aware datasets. It is not thread-safe for concurrent execution.
 */
public class TransactionContext {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionContext.class);

  private final Collection<TransactionAware> txAwares;
  private final TransactionSystemClient txClient;

  private Transaction currentTx;

  public TransactionContext(TransactionSystemClient txClient, TransactionAware... txAwares) {
    this(txClient, ImmutableList.copyOf(txAwares));
  }

  public TransactionContext(TransactionSystemClient txClient, Iterable<TransactionAware> txAwares) {
    // Use a set to avoid adding the same TransactionAware twice and to make removal faster.
    // Use a linked hash set so that insertion order is preserved (same behavior as when it was using a List).
    this.txAwares = Sets.newLinkedHashSet(txAwares);
    this.txClient = txClient;
  }

  /**
   * Adds a new transaction-aware to participate in the transaction.
   * @param txAware the new transaction-aware
   */
  public boolean addTransactionAware(TransactionAware txAware) {
    // If the txAware is newly added, call startTx as well if there is an active transaction
    boolean added = txAwares.add(txAware);
    if (added && currentTx != null) {
      txAware.startTx(currentTx);
    }
    return added;
  }

  /**
   * Removes a {@link TransactionAware} and withdraws from participation in the transaction.
   * Withdrawal is only allowed if there is no active transaction.
   *
   * @param txAware the {@link TransactionAware} to be removed
   * @return true if the given {@link TransactionAware} is removed; false otherwise.
   * @throws IllegalStateException if there is an active transaction going on with this TransactionContext.
   */
  public boolean removeTransactionAware(TransactionAware txAware) {
    Preconditions.checkState(currentTx == null, "Cannot remove TransactionAware while there is an active transaction.");
    return txAwares.remove(txAware);
  }

  /**
   * Starts a new transaction.  Calling this will initiate a new transaction using the {@link TransactionSystemClient},
   * and pass the returned transaction to {@link TransactionAware#startTx(Transaction)} for each registered
   * TransactionAware.  If an exception is encountered, the transaction will be aborted and a
   * {@code TransactionFailureException} wrapping the root cause will be thrown.
   *
   * @throws TransactionFailureException if an exception occurs starting the transaction with any registered
   *     TransactionAware
   */
  public void start() throws TransactionFailureException {
    currentTx = txClient.startShort();
    startAllTxAwares();
  }

  /**
   * Starts a new transaction.  Calling this will initiate a new transaction using the {@link TransactionSystemClient},
   * and pass the returned transaction to {@link TransactionAware#startTx(Transaction)} for each registered
   * TransactionAware.  If an exception is encountered, the transaction will be aborted and a
   * {@code TransactionFailureException} wrapping the root cause will be thrown.
   *
   * @param timeout the transaction timeout for the transaction
   *
   * @throws TransactionFailureException if an exception occurs starting the transaction with any registered
   *     TransactionAware
   */
  public void start(int timeout) throws TransactionFailureException {
    currentTx = txClient.startShort(timeout);
    startAllTxAwares();
  }

  /**
   * This is a helper for {@link #start()} and {@link #start(int)}.
   *
   * Passes the current transaction to {@link TransactionAware#startTx(Transaction)} for each registered
   * TransactionAware. If an exception is encountered, the transaction will be aborted and a
   * {@code TransactionFailureException} wrapping the root cause will be thrown.
   *
   * @throws TransactionFailureException if an exception occurs starting the transaction with any registered
   *     TransactionAware
   */
  private void startAllTxAwares() throws TransactionFailureException {
    for (TransactionAware txAware : txAwares) {
      try {
        txAware.startTx(currentTx);
      } catch (Throwable e) {
        String message = String.format("Unable to start transaction-aware '%s' for transaction %d. ",
                                       txAware.getTransactionAwareName(), currentTx.getTransactionId());
        LOG.warn(message, e);
        txClient.abort(currentTx);
        currentTx = null;
        throw new TransactionFailureException(message, e);
      }
    }
  }

  /**
   * Commits the current transaction.  This will: check for any conflicts, based on the change set aggregated from
   * all registered {@link TransactionAware} instances; flush any pending writes from the {@code TransactionAware}s;
   * commit the current transaction with the {@link TransactionSystemClient}; and clear the current transaction state.
   *
   * @throws TransactionConflictException if a conflict is detected with a recently committed transaction
   * @throws TransactionFailureException if an error occurs while committing
   */
  public void finish() throws TransactionFailureException {
    Preconditions.checkState(currentTx != null, "Cannot finish tx that has not been started");
    // each of these steps will abort and rollback the tx in case if errors, and throw an exception
    checkForConflicts();
    persist();
    commit();
    postCommit();
    currentTx = null;
  }

  /**
   * Aborts the given transaction, and rolls back all data set changes. If rollback fails,
   * the transaction is invalidated. If an exception is caught during rollback, the exception
   * is rethrown wrapped in a TransactionFailureException, after all remaining TransactionAwares have
   * completed rollback.
   *
   * @throws TransactionFailureException for any exception that is encountered.
   */
  public void abort() throws TransactionFailureException {
    abort(null);
  }

  /**
   * Checkpoints the current transaction by flushing any pending writes for the registered {@link TransactionAware}
   * instances, and obtaining a new current write pointer for the transaction.  By performing a checkpoint,
   * the client can ensure that all previous writes were flushed and are visible.  By default, the current write
   * pointer for the transaction is also visible.  The current write pointer can be excluded from read
   * operations by calling {@link Transaction#setVisibility(Transaction.VisibilityLevel)} with the visibility level set
   * to {@link Transaction.VisibilityLevel#SNAPSHOT_EXCLUDE_CURRENT} on the {@link Transaction} instance created
   * by the checkpoint call, which can be retrieved by calling {@link #getCurrentTransaction()}.
   *
   * After the checkpoint operation is performed, the updated
   * {@link Transaction} instance will be passed to {@link TransactionAware#startTx(Transaction)} for each
   * registered {@code TransactionAware} instance.
   *
   * @throws TransactionFailureException if an error occurs while performing the checkpoint
   */
  public void checkpoint() throws TransactionFailureException {
    Preconditions.checkState(currentTx != null, "Cannot checkpoint tx that has not been started");
    persist();
    try {
      currentTx = txClient.checkpoint(currentTx);
      // update the current transaction with all TransactionAwares
      for (TransactionAware txAware : txAwares) {
        txAware.updateTx(currentTx);
      }
    } catch (TransactionNotInProgressException e) {
      String message = String.format("Transaction %d is not in progress.", currentTx.getTransactionId());
      LOG.warn(message, e);
      abort(new TransactionFailureException(message, e));
      // abort will throw that exception
    } catch (Throwable e) {
      String message = String.format("Exception from checkpoint for transaction %d.", currentTx.getTransactionId());
      LOG.warn(message, e);
      abort(new TransactionFailureException(message, e));
      // abort will throw that exception
    }
  }

  /**
   * Returns the current transaction or null if no transaction is currently in progress.
   */
  @Nullable
  public Transaction getCurrentTransaction() {
    return currentTx;
  }

  // CHECKSTYLE IGNORE "@throws" FOR 11 LINES
  /**
   * Aborts the given transaction, and rolls back all data set changes. If rollback fails,
   * the transaction is invalidated. If an exception is caught during rollback, the exception
   * is rethrown wrapped into a TransactionFailureException, after all remaining TransactionAwares have
   * completed rollback. If an existing exception is passed in, that exception is thrown in either
   * case, whether the rollback is successful or not. In other words, this method always throws the
   * first exception that it encounters.
   * @param cause the original exception that caused the abort
   * @throws TransactionFailureException for any exception that is encountered.
   */
  public void abort(TransactionFailureException cause) throws TransactionFailureException {
    if (currentTx == null) {
      // might be called by some generic exception handler even though already aborted/finished - we allow that
      return;
    }
    try {
      boolean success = true;
      for (TransactionAware txAware : txAwares) {
        try {
          if (!txAware.rollbackTx()) {
            success = false;
          }
        } catch (Throwable e) {
          String message = String.format("Unable to roll back changes in transaction-aware '%s' for transaction %d. ",
                                         txAware.getTransactionAwareName(), currentTx.getTransactionId());
          LOG.warn(message, e);
          if (cause == null) {
            cause = new TransactionFailureException(message, e);
          }
          success = false;
        }
      }
      if (success) {
        txClient.abort(currentTx);
      } else {
        txClient.invalidate(currentTx.getTransactionId());
      }
      if (cause != null) {
        throw cause;
      }
    } finally {
      currentTx = null;
    }
  }

  private void checkForConflicts() throws TransactionFailureException {
    Collection<byte[]> changes = Lists.newArrayList();
    for (TransactionAware txAware : txAwares) {
      try {
        changes.addAll(txAware.getTxChanges());
      } catch (Throwable e) {
        String message = String.format("Unable to retrieve changes from transaction-aware '%s' for transaction %d. ",
                                       txAware.getTransactionAwareName(), currentTx.getTransactionId());
        LOG.warn(message, e);
        abort(new TransactionFailureException(message, e));
        // abort will throw that exception
      }
    }

    boolean canCommit = false;
    try {
      canCommit = txClient.canCommit(currentTx, changes);
    } catch (TransactionNotInProgressException e) {
      String message = String.format("Transaction %d is not in progress.", currentTx.getTransactionId());
      LOG.warn(message, e);
      abort(new TransactionFailureException(message, e));
      // abort will throw that exception
    } catch (Throwable e) {
      String message = String.format("Exception from canCommit for transaction %d.", currentTx.getTransactionId());
      LOG.warn(message, e);
      abort(new TransactionFailureException(message, e));
      // abort will throw that exception
    }
    if (!canCommit) {
      String message = String.format("Conflict detected for transaction %d.", currentTx.getTransactionId());
      abort(new TransactionConflictException(message));
      // abort will throw
    }
  }

  private void persist() throws TransactionFailureException {
    for (TransactionAware txAware : txAwares) {
      boolean success;
      Throwable cause = null;
      try {
        success = txAware.commitTx();
      } catch (Throwable e) {
        success = false;
        cause = e;
      }
      if (!success) {
        String message = String.format("Unable to persist changes of transaction-aware '%s' for transaction %d. ",
                                       txAware.getTransactionAwareName(), currentTx.getTransactionId());
        if (cause == null) {
          LOG.warn(message);
        } else {
          LOG.warn(message, cause);
        }
        abort(new TransactionFailureException(message, cause));
        // abort will throw that exception
      }
    }
  }

  private void commit() throws TransactionFailureException {
    boolean commitSuccess = false;
    try {
      commitSuccess = txClient.commit(currentTx);
    } catch (TransactionNotInProgressException e) {
      String message = String.format("Transaction %d is not in progress.", currentTx.getTransactionId());
      LOG.warn(message, e);
      abort(new TransactionFailureException(message, e));
      // abort will throw that exception
    } catch (Throwable e) {
      String message = String.format("Exception from commit for transaction %d.", currentTx.getTransactionId());
      LOG.warn(message, e);
      abort(new TransactionFailureException(message, e));
      // abort will throw that exception
    }
    if (!commitSuccess) {
      String message = String.format("Conflict detected for transaction %d.", currentTx.getTransactionId());
      abort(new TransactionConflictException(message));
      // abort will throw
    }
  }

  private void postCommit() throws TransactionFailureException {
    TransactionFailureException cause = null;
    for (TransactionAware txAware : txAwares) {
      try {
        txAware.postTxCommit();
      } catch (Throwable e) {
        String message = String.format("Unable to perform post-commit in transaction-aware '%s' for transaction %d. ",
                                       txAware.getTransactionAwareName(), currentTx.getTransactionId());
        LOG.warn(message, e);
        cause = new TransactionFailureException(message, e);
      }
    }
    if (cause != null) {
      throw cause;
    }
  }
}
