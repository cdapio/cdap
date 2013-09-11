package com.continuuity.data2.transaction;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Utility class that encapsulates the transaction life cycle over a given set of
 * transaction-aware datasets. The executor can be reused across multiple invocations
 * of the execute() method. However, it is not thread-safe for concurrent execution.
 */
public class TransactionExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionExecutor.class);

  final Collection<TransactionAware> txAwares;
  final TransactionSystemClient txClient;

  /**
   * Constructor for a transaction executor.
   */
  public TransactionExecutor(Collection<TransactionAware> txAwares, TransactionSystemClient txClient) {
    this.txAwares = ImmutableList.copyOf(txAwares);
    this.txClient = txClient;
  }

  /**
   * Execute a function under transactional semantics. A transaction is started  and all datasets
   * are initialized with the transaction. Then the passed function is executed, the transaction
   * is committed, and the function return value is returned as the return value of this method.
   * If any exception is caught, the transaction is aborted and the original exception is rethrown,
   * wrapped into a TransactionFailureException. If the transaction fails due to a write conflict,
   * a TransactionConflictException is thrown.
   * @param function the function to execute
   * @param input the input parameter for the function
   * @param <I> the input type of the function
   * @param <O> the result type of the function
   * @return the function's return value
   * @throws TransactionConflictException if there is a write conflict with another transaction.
   * @throws TransactionFailureException if any exception is caught, be it from the function or from the datasets.
   */
  public <I, O> O execute(Function<I, O> function, I input) throws TransactionFailureException {
    Transaction tx = start();
    O o = null;
    try {
      o = function.apply(input);
    } catch (Throwable e) {
      abort(tx, new TransactionFailureException("transaction function failure", e));
      // abort will throw
    }
    checkForConflicts(tx);
    persist(tx);
    if (!txClient.commit(tx)) {
      abort(tx, new TransactionConflictException("conflict detected"));
      // abort will throw
    }
    postCommit();
    return o;
  }

  private Transaction start() throws TransactionFailureException {
    Transaction tx = txClient.startShort();
    for (TransactionAware txAware : txAwares) {
      try {
        txAware.startTx(tx);
      } catch (Throwable e) {
        String message = "Unable to start transaction aware '" + txAware.getName() + "': ";
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
        String message = "Unable to retrieve changes from transaction aware '\" + txAware.getName() + \"': ";
        LOG.warn(message, e);
        abort(tx, new TransactionFailureException(message, e));
        // abort will throw that exception
      }
    }
    if (!txClient.canCommit(tx, changes)) {
      abort(tx, null);
      throw new TransactionConflictException("conflict detected");
    }
  }

  private void persist(Transaction tx) throws TransactionFailureException {
    for (TransactionAware txAware : txAwares) {
      try {
        txAware.commitTx();
      } catch (Throwable e) {
        String message = "Unable to persist changes of transaction aware '\" + txAware.getName() + \"': ";
        LOG.warn(message, e);
        abort(tx, new TransactionFailureException(message, e));
        // abort will throw that exception
      }
    }
  }

  private void postCommit() throws TransactionFailureException {
    for (TransactionAware txAware : txAwares) {
      try {
        txAware.postTxCommit();
      } catch (Throwable e) {
        String message = "Unable to perform post-commit in transaction aware '\" + txAware.getName() + \"': ";
        LOG.warn(message, e);
        throw new TransactionFailureException(message, e);
      }
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
        success = success && txAware.rollbackTx();
      } catch (Throwable e) {
        String message = "Unable to roll back changes in transaction aware '\" + txAware.getName() + \"'. ";
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
