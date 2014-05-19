package com.continuuity.data2.transaction;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

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
    this.txAwares = ImmutableList.copyOf(txAwares);
    this.txClient = txClient;
  }

  public void start() throws TransactionFailureException {
    currentTx = txClient.startShort();
    for (TransactionAware txAware : txAwares) {
      try {
        txAware.startTx(currentTx);
      } catch (Throwable e) {
        String message = String.format("Unable to start transaction-aware '%s' for transaction %d. ",
                                       txAware.getName(), currentTx.getWritePointer());
        LOG.warn(message, e);
        txClient.abort(currentTx);
        throw new TransactionFailureException(message, e);
      }
    }
  }

  public void finish() throws TransactionFailureException {
    Preconditions.checkState(currentTx != null, "Cannot finish tx that has not been started");
    // each of these steps will abort and rollback the tx in case if errors, and throw an exception
    checkForConflicts();
    persist();
    commit();
    postCommit();
    currentTx = null;
  }

  public void abort() throws TransactionFailureException {
    abort(null);
  }


  // CHECKSTYLE IGNORE "@throws" FOR 11 LINES
  /**
   * Aborts the given transaction, and rolls back all data set changes. If rollback fails,
   * the transaction is invalidated. If an exception is caught during rollback, the exception
   * is rethrown wrapped into a TransactionFailureException, after all remaining datasets have
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
                                         txAware.getName(), currentTx.getWritePointer());
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
        txClient.invalidate(currentTx.getWritePointer());
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
                                       txAware.getName(), currentTx.getWritePointer());
        LOG.warn(message, e);
        abort(new TransactionFailureException(message, e));
        // abort will throw that exception
      }
    }

    boolean canCommit = false;
    try {
      canCommit = txClient.canCommit(currentTx, changes);
    } catch (TransactionNotInProgressException e) {
      String message = String.format("Transaction %d is not in progress.", currentTx.getWritePointer());
      LOG.warn(message, e);
      abort(new TransactionFailureException(message, e));
      // abort will throw that exception
    } catch (Throwable e) {
      String message = String.format("Exception from canCommit for transaction %d.", currentTx.getWritePointer());
      LOG.warn(message, e);
      abort(new TransactionFailureException(message, e));
      // abort will throw that exception
    }
    if (!canCommit) {
      String message = String.format("Conflict detected for transaction %d.", currentTx.getWritePointer());
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
                                       txAware.getName(), currentTx.getWritePointer());
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
      String message = String.format("Transaction %d is not in progress.", currentTx.getWritePointer());
      LOG.warn(message, e);
      abort(new TransactionFailureException(message, e));
      // abort will throw that exception
    } catch (Throwable e) {
      String message = String.format("Exception from commit for transaction %d.", currentTx.getWritePointer());
      LOG.warn(message, e);
      abort(new TransactionFailureException(message, e));
      // abort will throw that exception
    }
    if (!commitSuccess) {
      String message = String.format("Conflict detected for transaction %d.", currentTx.getWritePointer());
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
                                       txAware.getName(), currentTx.getWritePointer());
        LOG.warn(message, e);
        cause = new TransactionFailureException(message, e);
      }
    }
    if (cause != null) {
      throw cause;
    }
  }
}
