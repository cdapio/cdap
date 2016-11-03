/*
 * Copyright © 2012-2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.transaction;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.tephra.AbstractTransactionExecutor;
import org.apache.tephra.RetryOnConflictStrategy;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.RetryStrategy;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;

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
 * This is a copy of Tephra's transaction executor, with the difference that it creates transaction
 * contexts using a supplier, instead of ceating it directly from a list of transaction-aware's. This
 * alow using it with a DynamicDatasetCache, where the set of datasets participating in transactions
 * can change while the transaction is executed. TODO: (CDAP-3988) remove this once TEPHRA-137 is done.
 */
public class DynamicTransactionExecutor extends AbstractTransactionExecutor {

  private final Supplier<TransactionContext> txContextSupplier;
  private final RetryStrategy retryStrategy;

  public DynamicTransactionExecutor(Supplier<TransactionContext> txContextSupplier, RetryStrategy retryStrategy) {
    super(MoreExecutors.sameThreadExecutor());
    this.txContextSupplier = txContextSupplier;
    this.retryStrategy = retryStrategy;
  }

  public DynamicTransactionExecutor(Supplier<TransactionContext> txContextSupplier) {
    this(txContextSupplier, RetryStrategies.retryOnConflict(20, 100));
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
    TransactionContext txContext;
    try {
      txContext = txContextSupplier.get();
    } catch (RuntimeException e) {
      // supplier.get() cannot throw checked exceptions. Therefore, the context supplier
      // must wrap a possible TransactionFailureException into a runtime exception.
      // this can happen, for example, if the supplier has an active transaction,
      // and this would represent a nested transaction.
      if (e.getCause() instanceof TransactionFailureException) {
        throw (TransactionFailureException) e.getCause();
      } else {
        throw e;
      }
    }
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
