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
 * contexts using a supplier, instead of creating it directly from a list of transaction-aware's. This
 * allow using it with a DynamicDatasetCache, where the set of datasets participating in transactions
 * can change while the transaction is executed. TODO: (CDAP-3988) remove this once TEPHRA-137 is done.
 */
public class DynamicTransactionExecutor extends AbstractTransactionExecutor {

  private final TransactionContextFactory txContextFactory;
  private final RetryStrategy retryStrategy;

  public DynamicTransactionExecutor(TransactionContextFactory txContextFactory, RetryStrategy retryStrategy) {
    super(MoreExecutors.sameThreadExecutor());
    this.txContextFactory = txContextFactory;
    this.retryStrategy = retryStrategy;
  }

  public DynamicTransactionExecutor(TransactionContextFactory txContextFactory) {
    this(txContextFactory, RetryStrategies.retryOnConflict(20, 100));
  }

  @Override
  public <I, O> O execute(Function<I, O> function, I input) throws TransactionFailureException, InterruptedException {
    return executeWithRetry(function, input);
  }

  @Override
  public <I> void execute(Procedure<I> procedure, I input) throws TransactionFailureException, InterruptedException {
    execute(input1 -> {
      procedure.apply(input1);
      return null;
    }, input);
  }

  @Override
  public <O> O execute(Callable<O> callable) throws TransactionFailureException, InterruptedException {
    return execute(input -> {
      return callable.call();
    }, null);
  }

  @Override
  public void execute(Subroutine subroutine) throws TransactionFailureException, InterruptedException {
    execute(input -> {
      subroutine.apply();
      return null;
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
    TransactionContext txContext = txContextFactory.newTransactionContext();
    txContext.start();
    O o = null;
    try {
      o = function.apply(input);
    } catch (Throwable e) {
      txContext.abort(new TransactionFailureException("Transaction function failure for transaction. ", e));
      // abort will throw
    }
    // will throw if something goes wrong
    txContext.finish();
    return o;
  }
}
