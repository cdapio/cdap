package com.continuuity.data2.transaction;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Utility class that encapsulates the transaction life cycle over a given set of
 * transaction-aware datasets. The executor can be reused across multiple invocations
 * of the execute() method. However, it is not thread-safe for concurrent execution.
 * <p>
 *   Transaction execution will be retries according to specified in constructor {@link RetryStrategy}.
 *   By default {@link RetryOnConflictStrategy} is used with max 20 retries and 100 ms between retries.
 * </p>
 */
public class DefaultTransactionExecutor extends AbstractTransactionExecutor {

  private final Collection<TransactionAware> txAwares;
  private final TransactionSystemClient txClient;
  private final RetryStrategy retryStrategy;

  /**
   * Convenience constructor, has same affect as {@link #DefaultTransactionExecutor(TransactionSystemClient, Iterable)}
   */
  public DefaultTransactionExecutor(TransactionSystemClient txClient, TransactionAware... txAwares) {
    this(txClient, Arrays.asList(txAwares));
  }


  public DefaultTransactionExecutor(TransactionSystemClient txClient,
                                    Iterable<TransactionAware> txAwares,
                                    RetryStrategy retryStrategy) {

    super(MoreExecutors.sameThreadExecutor());
    this.txAwares = ImmutableList.copyOf(txAwares);
    this.txClient = txClient;
    this.retryStrategy = retryStrategy;
  }

  /**
   * Constructor for a transaction executor.
   */
  @Inject
  public DefaultTransactionExecutor(TransactionSystemClient txClient, @Assisted Iterable<TransactionAware> txAwares) {
    this(txClient, txAwares, RetryStrategies.retryOnConflict(20, 100));
  }

  @Override
  public <I, O> O execute(Function<I, O> function, I input) throws TransactionFailureException, InterruptedException {
    return executeWithRetry(function, input);
  }

  @Override
  public <I> void execute(final Procedure<I> procedure, I input)
    throws TransactionFailureException, InterruptedException {

    execute(new Function<I, Void>() {
      @Override
      public Void apply(I input) throws Exception {
        procedure.apply(input);
        return null;
      }
    }, input);
  }

  @Override
  public <O> O execute(final Callable<O> callable) throws TransactionFailureException, InterruptedException {
    return execute(new Function<Void, O>() {
      @Override
      public O apply(Void input) throws Exception {
        return callable.call();
      }
    }, null);
  }

  @Override
  public void execute(final Subroutine subroutine) throws TransactionFailureException, InterruptedException {
    execute(new Function<Void, Void>() {
      @Override
      public Void apply(Void input) throws Exception {
        subroutine.apply();
        return null;
      }
    }, null);
  }

  private <I, O> O executeWithRetry(Function<I, O> function, I input)
    throws TransactionFailureException, InterruptedException {

    int retries = 0;
    while (true) {
      try {
        return executeOnce(function, input);
      } catch (TransactionFailureException e) {
        long delay = retryStrategy.nextRetry(e, ++retries);

        if (delay < 0) {
          throw e;
        }

        if (delay > 0) {
          TimeUnit.MILLISECONDS.sleep(delay);
        }
      }
    }

  }

  private <I, O> O executeOnce(Function<I, O> function, I input) throws TransactionFailureException {
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
