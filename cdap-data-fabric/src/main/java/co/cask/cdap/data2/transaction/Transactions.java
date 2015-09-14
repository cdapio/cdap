/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * Helper class for interacting with {@link Transaction} and {@link TransactionSystemClient}.
 */
public final class Transactions {

  private static final Logger LOG = LoggerFactory.getLogger(Transactions.class);

  public static void abortQuietly(TransactionSystemClient txClient, Transaction tx) {
    try {
      txClient.abort(tx);
    } catch (Throwable t) {
      LOG.error("Exception when aborting transaction {}", tx, t);
    }
  }

  public static void invalidateQuietly(TransactionSystemClient txClient, Transaction tx) {
    try {
      txClient.invalidate(tx.getWritePointer());
    } catch (Throwable t) {
      LOG.error("Exception when invalidating transaction {}", tx, t);
    }
  }

  /**
   * Handy method to create {@link TransactionExecutor} (See TEPHRA-71).
   */
  public static TransactionExecutor createTransactionExecutor(TransactionExecutorFactory factory,
                                                              Iterable<? extends TransactionAware> txAwares) {
    return factory.createExecutor(Iterables.transform(txAwares, Functions.<TransactionAware>identity()));
  }

  /**
   * Handy method to create {@link TransactionExecutor} with single {@link TransactionAware}.
   */
  public static TransactionExecutor createTransactionExecutor(TransactionExecutorFactory factory,
                                                              TransactionAware txAware) {
    return factory.createExecutor(ImmutableList.of(txAware));
  }

  /**
   * Executes the given {@link Runnable} in a short transaction using the given {@link TransactionContext}.
   *
   * @param txContext the {@link TransactionContext} for managing the transaction lifecycle
   * @param name descriptive name for the runnable
   * @param runnable the Runnable to be executed inside a transaction
   * @throws TransactionFailureException if failed to execute in a transaction. The cause of the exception carries the
   *                                     reason of failure.
   */
  public static void execute(TransactionContext txContext, String name,
                             final Runnable runnable) throws TransactionFailureException {
    execute(txContext, name, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        runnable.run();
        return null;
      }
    });
  }

  /**
   * Executes the given {@link Callable} in a short transaction using the given {@link TransactionContext}.
   *
   * @param txContext the {@link TransactionContext} for managing the transaction lifecycle
   * @param name descriptive name for the runnable
   * @param callable the Callable to be executed inside a transaction
   * @return the result returned by {@link Callable#call()}
   * @throws TransactionFailureException if failed to execute in a transaction. The cause of the exception carries the
   *                                     reason of failure.
   */
  public static <V> V execute(TransactionContext txContext, String name,
                              Callable<V> callable) throws TransactionFailureException {
    V result = null;
    txContext.start();
    try {
      result = callable.call();
    } catch (Throwable t) {
      // Abort will always throw with the TransactionFailureException.
      txContext.abort(new TransactionFailureException("Failed to execute method " + name + " inside a transaction", t));
    }

    // If commit failed, the tx will be aborted and exception will be raised
    txContext.finish();
    return result;
  }

  private Transactions() {
    // Private the constructor for util class
  }
}
