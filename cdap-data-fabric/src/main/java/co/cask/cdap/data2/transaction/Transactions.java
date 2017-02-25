/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
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
import java.util.concurrent.atomic.AtomicReference;

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
   * Executes the given {@link TxCallable} using the given {@link Transactional}.
   *
   * @param transactional the {@link Transactional} to use for transactional execution.
   * @param callable the {@link TxCallable} to be executed inside a transaction
   * @param <V> type of the result
   * @return value returned by the given {@link TxCallable}.
   * @throws TransactionFailureException if failed to execute the given {@link TxRunnable} in a transaction
   *
   * TODO: CDAP-6103 Move this to {@link Transactional} when revamping tx supports in program.
   */
  public static <V> V execute(Transactional transactional,
                              final TxCallable<V> callable) throws TransactionFailureException {
    final AtomicReference<V> result = new AtomicReference<>();
    transactional.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        result.set(callable.call(context));
      }
    });
    return result.get();
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

  /**
   * Propagates the given {@link TransactionFailureException}. If the {@link TransactionFailureException#getCause()}
   * doesn't return {@code null}, the cause will be used instead for the propagation. This method will
   * throw the failure exception as-is if it is a {@link RuntimeException}. Otherwise, the exception will be wrapped
   * with inside a {@link RuntimeException}.
   * This method will always throw and the returned exception is for satisfying Java static analysis only.
   *
   * @param e the {@link TransactionFailureException} to propagate
   * @return a {@link RuntimeException}
   */
  public static RuntimeException propagate(TransactionFailureException e) {
    throw Throwables.propagate(Objects.firstNonNull(e.getCause(), e));
  }

  /**
   * Propagates the given {@link TransactionFailureException}. If the {@link TransactionFailureException#getCause()}
   * doesn't return {@code null}, the cause will be used instead for the propagation. This method will
   * throw the failure exception as-is the given propagated type if the type matches or as {@link RuntimeException}.
   * This method will always throw and the returned exception is for satisfying Java static analysis only.
   *
   * @param e the {@link TransactionFailureException} to propagate
   * @param propagateType if the exception is an instance of this type, it will be rethrown as is
   * @param <X> exception type of propagate type
   * @return a exception of type X.
   */
  public static <X extends Throwable> X propagate(TransactionFailureException e,
                                                  Class<X> propagateType) throws X {
    Throwable cause = Objects.firstNonNull(e.getCause(), e);
    Throwables.propagateIfPossible(cause, propagateType);
    throw Throwables.propagate(cause);
  }

  /**\
   * Propagates the given {@link TransactionFailureException}. If the {@link TransactionFailureException#getCause()}
   * doesn't return {@code null}, the cause will be used instead for the propagation. This method will
   * throw the failure exception as-is the given propagated types if the type matches or as {@link RuntimeException}.
   * This method will always throw and the returned exception is for satisfying Java static analysis only.
   *
   * @param e the {@link TransactionFailureException} to propagate
   * @param propagateType1 if the exception is an instance of this type, it will be rethrown as is
   * @param propagateType2 if the exception is an instance of this type, it will be rethrown as is
   * @param <X1> exception type of first propagate type
   * @param <X2> exception type of second propagate type
   * @return a exception of type X1.
   */
  public static <X1 extends Throwable, X2 extends Throwable> X1 propagate(TransactionFailureException e,
                                                                          Class<X1> propagateType1,
                                                                          Class<X2> propagateType2) throws X1, X2 {
    Throwable cause = Objects.firstNonNull(e.getCause(), e);
    Throwables.propagateIfPossible(cause, propagateType1, propagateType2);
    throw Throwables.propagate(cause);
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

  /**
   * Executes the given callable with a transaction. Think twice before you call this. Usages of this method
   * likely indicate poor exception handling.
   *
   * @param transactional the {@link Transactional} used to submit the task
   * @param callable the task
   * @param <V> type of result
   * @return value returned by the given {@link TxCallable}.
   * @throws RuntimeException for any errors that occur
   */
  public static <V> V executeUnchecked(Transactional transactional, final TxCallable<V> callable) {
    try {
      return execute(transactional, callable);
    } catch (TransactionFailureException e) {
      throw propagate(e);
    }
  }

  /**
   * Executes the given runnable with a transaction. Think twice before you call this. Usages of this method
   * likely indicate poor exception handling.
   *
   * @param transactional the {@link Transactional} used to submit the task
   * @param runnable task
   * @throws RuntimeException for errors that occur
   */
  public static void executeUnchecked(Transactional transactional, final TxRunnable runnable) {
    executeUnchecked(transactional, new TxCallable<Void>() {
      @Override
      public Void call(DatasetContext context) throws Exception {
        runnable.run(context);
        return null;
      }
    });
  }
}
