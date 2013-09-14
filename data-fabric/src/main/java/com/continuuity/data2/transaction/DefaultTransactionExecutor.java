package com.continuuity.data2.transaction;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
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
  @Override
  public <I, O> O execute(Function<I, O> function, I input) throws TransactionFailureException {
    TransactionContext txContext = new TransactionContext(txClient, txAwares);
    txContext.start();
    O o = null;
    try {
      o = function.apply(input);
    } catch (Throwable e) {
      txContext.abort(new TransactionFailureException("Transaction function failure for transaction. ", e));
      // abort will throw
    }
    // will throw if smth goes wrong
    txContext.finish();
    return o;
  }
}
