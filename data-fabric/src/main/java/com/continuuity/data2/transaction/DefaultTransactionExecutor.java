package com.continuuity.data2.transaction;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import java.util.Collection;
import java.util.concurrent.Callable;

/**
 * Utility class that encapsulates the transaction life cycle over a given set of
 * transaction-aware datasets. The executor can be reused across multiple invocations
 * of the execute() method. However, it is not thread-safe for concurrent execution.
 */
public class DefaultTransactionExecutor extends AbstractTransactionExecutor {

  private final Collection<TransactionAware> txAwares;
  private final TransactionSystemClient txClient;

  /**
   * Constructor for a transaction executor.
   */
  @Inject
  public DefaultTransactionExecutor(TransactionSystemClient txClient, @Assisted Iterable<TransactionAware> txAwares) {
    super(MoreExecutors.sameThreadExecutor());
    this.txAwares = ImmutableList.copyOf(txAwares);
    this.txClient = txClient;
  }

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
  public <O> O execute(final Callable<O> callable) throws TransactionFailureException {
    return execute(new Function<Void, O>() {
      @Override
      public O apply(Void input) throws Exception {
        return callable.call();
      }
    }, null);
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
}
