package com.continuuity.data2.transaction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Utility class that encapsulates the transaction life cycle over a given set of
 * transaction-aware datasets. The executor can be reused across multiple invocations
 * of the execute() method. However, it is not thread-safe for concurrent execution.
 */
public class DefaultTransactionExecutor implements TransactionExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultTransactionExecutor.class);

  private final Collection<TransactionAware> txAwares;
  private final TransactionSystemClient txClient;

  /**
   * Constructor for a transaction executor.
   */
  @Inject
  public DefaultTransactionExecutor(TransactionSystemClient txClient, @Assisted Iterable<TransactionAware> txAwares) {
    this.txAwares = ImmutableList.copyOf(txAwares);
    this.txClient = txClient;
  }

  @Override
  public <I, O> O execute(Function<I, O> function, I input) throws TransactionFailureException {
    Transaction tx = start();
    O o = null;
    try {
      o = function.apply(input);
    } catch (Throwable e) {
      String message = String.format("Transaction function failure for transaction %d. ", tx.getWritePointer());
      abort(tx, new TransactionFailureException(message, e));
      // abort will throw
    }
    // each of these steps will abort and rollback the tx in case if errors, and throw an exception
    checkForConflicts(tx);
    persist(tx);
    commit(tx);
    postCommit(tx);
    return o;
  }

  @Override
  public <I> void execute(final Procedure<I> procedure, I input) throws TransactionFailureException {
    execute(new Function<I, Void>() {
      @Override
      public Void apply(I input) throws Exception {
        procedure.apply(input);
        return null;
      }
    }, input);
  }

  @Override
  public void execute(final Subroutine subroutine) throws TransactionFailureException {
    execute(new Function<Void, Void>() {
      @Override
      public Void apply(Void input) throws Exception {
        subroutine.apply();
        return null;
      }
    }, null);
  }

  private Transaction start() throws TransactionFailureException {
    Transaction tx = txClient.startShort();
    for (TransactionAware txAware : txAwares) {
      try {
        txAware.startTx(tx);
      } catch (Throwable e) {
        String message = String.format("Unable to start transaction-aware '%s' for transaction %d. ",
                                       txAware.getName(), tx.getWritePointer());
        LOG.warn(message, e);
        txClient.abort(tx);
        throw new TransactionFailureException(message, e);
      }
    }
    return tx;
  }

  private void checkForConflicts(Transaction tx) throws TransactionFailureException {
    Collection<byte[]> changes = Lists.newArrayList();
    for (TransactionAware txAware : txAwares) {
      try {
        changes.addAll(txAware.getTxChanges());
      } catch (Throwable e) {
        String message = String.format("Unable to retrieve changes from transaction-aware '%s' for transaction %d. ",
                                       txAware.getName(), tx.getWritePointer());
        LOG.warn(message, e);
        abort(tx, new TransactionFailureException(message, e));
        // abort will throw that exception
      }
    }

    boolean canCommit = false;
    try {
      canCommit = txClient.canCommit(tx, changes);
    } catch (Throwable e) {
      String message = String.format("Exception from canCommit for transaction %d.", tx.getWritePointer());
      LOG.warn(message, e);
      abort(tx, new TransactionFailureException(message, e));
      // abort will throw that exception
    }
    if (!canCommit) {
      String message = String.format("Conflict detected for transaction %d.", tx.getWritePointer());
      abort(tx, new TransactionConflictException(message));
      // abort will throw
    }
  }

  private void persist(Transaction tx) throws TransactionFailureException {
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
                                       txAware.getName(), tx.getWritePointer());
        if (cause == null) {
          LOG.warn(message);
        } else {
          LOG.warn(message, cause);
        }
        abort(tx, new TransactionFailureException(message, cause));
        // abort will throw that exception
      }
    }
  }

  private void commit(Transaction tx) throws TransactionFailureException {
    boolean commitSuccess = false;
    try {
      commitSuccess = txClient.commit(tx);
    } catch (Throwable e) {
      String message = String.format("Exception from commit for transaction %d.", tx.getWritePointer());
      LOG.warn(message, e);
      abort(tx, new TransactionFailureException(message, e));
      // abort will throw that exception
    }
    if (!commitSuccess) {
      String message = String.format("Conflict detected for transaction %d.", tx.getWritePointer());
      abort(tx, new TransactionConflictException(message));
      // abort will throw
    }
  }

  private void postCommit(Transaction tx) throws TransactionFailureException {
    TransactionFailureException cause = null;
    for (TransactionAware txAware : txAwares) {
      try {
        txAware.postTxCommit();
      } catch (Throwable e) {
        String message = String.format("Unable to perform post-commit in transaction-aware '%s' for transaction %d. ",
                                       txAware.getName(), tx.getWritePointer());
        LOG.warn(message, e);
        cause = new TransactionFailureException(message, e);
      }
    }
    if (cause != null) {
      throw cause;
    }
  }

  /**
   * Aborts the given transaction, and rolls back all data set changes. If rollback fails,
   * the transaction is invalidated. If an exception is caught during rollback, the exception
   * is rethrown wrapped into a TransactionFailureException, after all remaining datasets have
   * completed rollback. If an existing exception is passed in, that exception is thrown in either
   * case, whether the rollback is successful or not. In other words, this method always throws the
   * first exception that it encounters.
   * @param tx the transaction to roll back
   * @param cause the original exception that caused the abort
   * @throws TransactionFailureException for any exception that is encountered.
   */
  private void abort(Transaction tx, TransactionFailureException cause) throws TransactionFailureException {
    boolean success = true;
    for (TransactionAware txAware : txAwares) {
      try {
        if (!txAware.rollbackTx()) {
          success = false;
        }
      } catch (Throwable e) {
        String message = String.format("Unable to roll back changes in transaction-aware '%s' for transaction %d. ",
                                       txAware.getName(), tx.getWritePointer());
        LOG.warn(message, e);
        if (cause == null) {
          cause = new TransactionFailureException(message, e);
        }
        success = false;
      }
    }
    if (success) {
      txClient.abort(tx);
    } else {
      txClient.invalidate(tx);
    }
    if (cause != null) {
      throw cause;
    }
  }
}
