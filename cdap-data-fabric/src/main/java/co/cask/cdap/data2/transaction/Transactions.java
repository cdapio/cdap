/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import com.google.common.base.Functions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.tephra.RetryStrategy;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Helper class for interacting with {@link Transaction} and {@link TransactionSystemClient}.
 */
public final class Transactions {

  private static final Logger LOG = LoggerFactory.getLogger(Transactions.class);

  /**
   * Invalidates the given transaction without throwing any exception. If there is exception raised during invalidation,
   * it will get logged as an error.
   */
  public static void invalidateQuietly(TransactionSystemClient txClient, Transaction tx) {
    try {
      if (!txClient.invalidate(tx.getWritePointer())) {
        LOG.error("Failed to invalidate transaction {}", tx);
      }
    } catch (Throwable t) {
      LOG.error("Exception when invalidating transaction {}", tx, t);
    }
  }

  /**
   * Wraps the given {@link Throwable} as a {@link TransactionFailureException} if it is not already an instance of
   * {@link TransactionFailureException}.
   */
  public static TransactionFailureException asTransactionFailure(Throwable t) {
    return asTransactionFailure(t, "Exception raised in transactional execution. Cause: " + t.getMessage());
  }

  /**
   * Wraps the given {@link Throwable} as a {@link TransactionFailureException} if it is not already an instance of
   * {@link TransactionFailureException}.
   *
   * @param t the original exception
   * @param message the exception message to use in case wrapping is needed
   */
  public static TransactionFailureException asTransactionFailure(Throwable t, String message) {
    if (t instanceof TransactionFailureException) {
      return (TransactionFailureException) t;
    }
    return new TransactionFailureException(message, t);
  }

  /**
   * Handy method to create {@link TransactionExecutor} (See TEPHRA-71).
   */
  public static TransactionExecutor createTransactionExecutor(TransactionExecutorFactory factory,
                                                              final TransactionSystemClient txClient,
                                                              final Iterable<? extends TransactionAware> txAwares) {
    return factory.createExecutor(new Supplier<TransactionContext>() {
      @Override
      public TransactionContext get() {
        return new TransactionContext(txClient, Iterables.transform(txAwares, Functions.<TransactionAware>identity()));
      }
    });
  }

  /**
   * Handy method to create {@link TransactionExecutor} with single {@link TransactionAware}.
   */
  public static TransactionExecutor createTransactionExecutor(TransactionExecutorFactory factory,
                                                              TransactionSystemClient txClient,
                                                              TransactionAware txAware) {
    return createTransactionExecutor(factory, txClient, ImmutableList.of(txAware));
  }

  /**
   * Handy method to create {@link TransactionExecutor} (See TEPHRA-71).
   */
  public static TransactionExecutor createTransactionExecutor(org.apache.tephra.TransactionExecutorFactory factory,
                                                              Iterable<? extends TransactionAware> txAwares) {
    return factory.createExecutor(Iterables.transform(txAwares, Functions.<TransactionAware>identity()));
  }

  public static TransactionExecutor createTransactionExecutor(org.apache.tephra.TransactionExecutorFactory factory,
                                                              TransactionAware txAware) {
    return factory.createExecutor(Collections.singleton(txAware));
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
    execute(txContext, name, (Callable<Void>) () -> {
      runnable.run();
      return null;
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

  /**
   * Creates a new instance of {@link Transactional} for {@link TxRunnable} execution using the
   * given {@link DynamicDatasetCache}.
   *
   * @param datasetCache The {@link DynamicDatasetCache} to use fo transaction creation as well as provided to the
   *                     {@link TxRunnable} for access to dataset
   * @return a new instance of {@link Transactional}
   */
  public static Transactional createTransactional(final DynamicDatasetCache datasetCache) {
    return new CacheBasedTransactional(datasetCache);
  }

  /**
   * Creates a new instance of {@link Transactional} for {@link TxRunnable} execution using the
   * given {@link DynamicDatasetCache}.
   *
   * @param datasetCache The {@link DynamicDatasetCache} to use fo transaction creation as well as provided to the
   *                     {@link TxRunnable} for access to dataset
   * @param defaultTimeout The default transaction timeout, used for starting the transaction for
   *                       {@link Transactional#execute(TxRunnable)}
   * @return a new instance of {@link Transactional}
   */
  public static Transactional createTransactional(final DynamicDatasetCache datasetCache,
                                                  final int defaultTimeout) {
    return new CacheBasedTransactional(datasetCache) {
      @Override
      protected void startTransaction(TransactionContext txContext) throws TransactionFailureException {
        txContext.start(defaultTimeout);
      }
    };
  }

  private static class CacheBasedTransactional implements Transactional {

    private final DynamicDatasetCache datasetCache;

    CacheBasedTransactional(DynamicDatasetCache cache) {
      this.datasetCache = cache;
    }

    /**
     * Starts a short transaction with default timeout. Subclasses can override this to change the default.
     */
    protected void startTransaction(TransactionContext txContext) throws TransactionFailureException {
      txContext.start();
    }

    @Override
    public void execute(TxRunnable runnable) throws TransactionFailureException {
      TransactionContext txContext = datasetCache.newTransactionContext();
      startTransaction(txContext);
      finishExecute(txContext, runnable);
    }

    @Override
    public void execute(int timeout, TxRunnable runnable) throws TransactionFailureException {
      TransactionContext txContext = datasetCache.newTransactionContext();
      txContext.start(timeout);
      finishExecute(txContext, runnable);
    }

    private void finishExecute(TransactionContext txContext, TxRunnable runnable)
      throws TransactionFailureException {
      try {
        runnable.run(datasetCache);
      } catch (Exception e) {
        txContext.abort(new TransactionFailureException("Exception raised from TxRunnable.run() " + runnable, e));
      }
      // The call the txContext.abort above will always have exception thrown
      // Hence we'll only reach here if and only if the runnable.run() returns normally.
      txContext.finish();
    }
  }

  /**
   * Creates a new {@link Transactional} that will automatically retry upon transaction failure.
   *
   * @param transactional The {@link Transactional} to delegate the transaction execution to
   * @param retryStrategy the {@link RetryStrategy} to use when there is a {@link TransactionFailureException}
   *                      raised from the transaction execution.
   * @return a new instance of {@link Transactional}.
   */
  public static Transactional createTransactionalWithRetry(final Transactional transactional,
                                                           final RetryStrategy retryStrategy) {
    return new Transactional() {

      @Override
      public void execute(TxRunnable runnable) throws TransactionFailureException {
        executeInternal(null, runnable);
      }

      @Override
      public void execute(int timeoutInSeconds, TxRunnable runnable) throws TransactionFailureException {
        executeInternal(timeoutInSeconds, runnable);
      }

      private void executeInternal(Integer timeout, TxRunnable runnable) throws TransactionFailureException {
        int retries = 0;
        while (true) {
          try {
            if (null == timeout) {
              transactional.execute(runnable);
            } else {
              transactional.execute(timeout, runnable);
            }
            break;
          } catch (TransactionFailureException e) {
            long delay = retryStrategy.nextRetry(e, ++retries);

            if (delay < 0) {
              throw e;
            }

            if (delay > 0) {
              try {
                TimeUnit.MILLISECONDS.sleep(delay);
              } catch (InterruptedException e1) {
                // Reinstate the interrupt thread
                Thread.currentThread().interrupt();
                // Fail with the original TransactionFailureException
                throw e;
              }
            }
          }
        }
      }
    };
  }

  private Transactions() {
    // Private the constructor for util class
  }

  /**
   * Determines the transaction control of a method, as (optionally) annotated with @TransactionPolicy.
   * If the program's class does not implement the method, the superclass is inspected up to and including
   * the given base class.
   *
   * @param defaultValue returned if no annotation is present
   * @param baseClass upper bound for the super classes to be inspected
   * @param program the program
   * @param methodName the name of the method
   * @param params the parameter types of the method
   *
   * @return the transaction control annotated for the method of the program class or its nearest superclass that
   *         is annotated; or defaultValue if no annotation is present in any of the superclasses
   */
  public static TransactionControl getTransactionControl(TransactionControl defaultValue, Class<?> baseClass,
                                                         Object program, String methodName, Class<?>... params) {
    Class<?> cls = program.getClass();
    while (Object.class != cls) { // we know that Object cannot have this annotation :)
      TransactionControl txControl = getTransactionControl(cls, methodName, params);
      if (txControl != null) {
        return txControl;
      }
      if (cls == baseClass) {
        break; // reached bounding superclass
      }
      cls = cls.getSuperclass();
    }
    // if we reach this point, then none of the super classes had an annotation for the method
    for (Class<?> interf : program.getClass().getInterfaces()) {
      TransactionControl txControl = getTransactionControl(interf, methodName, params);
      if (txControl != null) {
        return txControl;
      }
    }
    return defaultValue;
  }

  private static TransactionControl getTransactionControl(Class<?> cls, String methodName, Class<?>[] params) {
    try {
      Method method = cls.getDeclaredMethod(methodName, params);
      TransactionPolicy annotation = method.getAnnotation(TransactionPolicy.class);
      if (annotation != null) {
        return annotation.value();
      }
    } catch (NoSuchMethodException e) {
      // this class does not have the method, that is ok
    }
    return null;
  }

}
