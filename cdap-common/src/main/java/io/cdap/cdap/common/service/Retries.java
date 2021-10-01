/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

package io.cdap.cdap.common.service;

import io.cdap.cdap.api.retry.RetriesExhaustedException;
import io.cdap.cdap.api.retry.RetryFailedException;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.logging.LogSamplers;
import io.cdap.cdap.common.logging.Loggers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Utilities to perform logic with retries.
 */
public final class Retries {
  private static final Logger LOG = LoggerFactory.getLogger(Retries.class);

  public static final Predicate<Throwable> ALWAYS_TRUE = t -> true;
  public static final Predicate<Throwable> DEFAULT_PREDICATE = RetryableException.class::isInstance;

  private Retries() {

  }

  /**
   * A Callable whose throwable is generic. This is used so that
   * {@link #callWithRetries(Callable, RetryStrategy, Predicate)} does not have to wrap any exceptions.
   *
   * @param <V> the type of return value
   * @param <T> the type of throwable
   */
  public interface Callable<V, T extends Throwable> {
    V call() throws T;
  }

  /**
   * A Runnable whose throwable is generic. This is used so that
   * {@link #runWithRetries(Runnable, RetryStrategy, Predicate)} does not have to wrap any exceptions.
   *
   * @param <T> the type of throwable
   */
  public interface Runnable<T extends Throwable> {
    void run() throws T;
  }

  /**
   * A Callable whose throwable is generic and call method takes attempt count as argument.
   * This is helpful for capturing retry count
   *
   * @param <V> the type of return value
   * @param <T> the type of throwable
   */
  public interface CallableWithContext<V, T extends Throwable> {
    V call(RetryContext retryContext) throws T;
  }

  /**
   * The same as calling {@link #supplyWithRetries(Supplier, RetryStrategy, Predicate)} where a retryable failure
   * is defined as a {@link RetryableException}.
   *
   * @param supplier the callable to run
   * @param retryStrategy the retry strategy to use if the supplier throws a {@link RetryableException}
   * @param <V> the type of object returned by the supplier
   * @return the return value of the supplier
   * @throws RuntimeException if the supplier failed in a way that is not retryable, or the retries were exhausted.
   *   If retries were exhausted, a {@link RetriesExhaustedException} will be added as a suppressed exception.
   *   If the call was interrupted while waiting between retries, the {@link InterruptedException} will be added
   *   as a suppressed exception
   */
  public static <V> V supplyWithRetries(Supplier<V> supplier, RetryStrategy retryStrategy) {
    return supplyWithRetries(supplier, retryStrategy, DEFAULT_PREDICATE);
  }

  /**
   * Executes {@link Supplier#get()}, retrying the call if it throws something retryable. This is similar to
   * {@link #callWithRetries(Callable, RetryStrategy, Predicate)}, except it will simply re-throw any non-retryable
   * exception instead of wrapping it in an ExecutionException. If you need to run logic that throws a
   * checked exception, use {@link #callWithRetries(Callable, RetryStrategy, Predicate)} instead.
   *
   * @param supplier the callable to run
   * @param retryStrategy the retry strategy to use if the supplier fails in a retryable way
   * @param isRetryable predicate to determine whether the supplier failure is retryable or not
   * @param <V> the type of object returned by the supplier
   * @return the return value of the supplier
   * @throws RuntimeException if the supplier failed in a way that is not retryable, or the retries were exhausted.
   *   If retries were exhausted, a {@link RetriesExhaustedException} will be added as a suppressed exception.
   *   If the call was interrupted while waiting between retries, the {@link InterruptedException} will be added
   *   as a suppressed exception
   */
  public static <V> V supplyWithRetries(Supplier<V> supplier, RetryStrategy retryStrategy,
                                        Predicate<Throwable> isRetryable) {
    return callWithRetries(supplier::get, retryStrategy, isRetryable);
  }

  /**
   * The same as calling {@link #runWithRetries(Runnable, RetryStrategy, Predicate)} where a retryable failure
   * is defined as a {@link RetryableException}.
   *
   * @param runnable the callable to run
   * @param retryStrategy the retry strategy to use if the supplier fails in a retryable way
   * @param <T> the type of throwable
   * @throws T if the runnable failed in a way that is not retryable, or the retries were exhausted.
   *   If retries were exhausted, a {@link RetriesExhaustedException} will be added as a suppressed exception.
   *   If the call was interrupted while waiting between retries, the {@link InterruptedException} will be added
   *   as a suppressed exception
   */
  public static <T extends Throwable> void runWithRetries(Runnable<T> runnable, RetryStrategy retryStrategy) throws T {
    runWithRetries(runnable, retryStrategy, DEFAULT_PREDICATE);
  }

  /**
   * Executes {@link Runnable#run()}, retrying the call if it throws something retryable.
   *
   * @param runnable the callable to run
   * @param retryStrategy the retry strategy to use if the supplier fails in a retryable way
   * @param isRetryable predicate to determine whether the supplier failure is retryable or not
   * @param <T> the type of throwable
   * @throws T if the runnable failed in a way that is not retryable, or the retries were exhausted.
   *   If retries were exhausted, a {@link RetriesExhaustedException} will be added as a suppressed exception.
   *   If the call was interrupted while waiting between retries, the {@link InterruptedException} will be added
   *   as a suppressed exception
   */
  public static <T extends Throwable> void runWithRetries(Runnable<T> runnable, RetryStrategy retryStrategy,
                                                          Predicate<Throwable> isRetryable) throws T {
    callWithRetries((Callable<Void, T>) () -> {
      runnable.run();
      return null;
    }, retryStrategy, isRetryable);
  }

  /**
   * The same as calling {@link #callWithRetries(Callable, RetryStrategy, Predicate)} where a retryable failure
   * is defined as a {@link RetryableException}.
   *
   * @param callable the callable to run
   * @param retryStrategy the retry strategy to use if the callable throws a {@link RetryableException}
   * @param <V> the type of return value
   * @param <T> the type of throwable
   * @return the return value of the callable
   * @throws T if the callable failed in a way that is not retryable, or the retries were exhausted.
   *   If retries were exhausted, a {@link RetriesExhaustedException} will be added as a suppressed exception.
   *   If the call was interrupted while waiting between retries, the {@link InterruptedException} will be added
   *   as a suppressed exception
   */
  public static <V, T extends Throwable> V callWithRetries(Callable<V, T> callable,
                                                           RetryStrategy retryStrategy) throws T {
    return callWithRetries(callable, retryStrategy, DEFAULT_PREDICATE);
  }

  /**
   * Same as calling {@link #callWithRetries(CallableWithContext, RetryStrategy, Predicate)} with attempt count ignored
   *
   * @param callable the callable to run
   * @param retryStrategy the retry strategy to use if the callable fails in a retryable way
   * @param isRetryable predicate to determine whether the callable failure is retryable or not
   * @param <V> the type of return value
   * @param <T> the type of throwable
   * @return the return value of the callable
   * @throws T if the callable failed in a way that is not retryable, or the retries were exhausted.
   *   If retries were exhausted, a {@link RetriesExhaustedException} will be added as a suppressed exception.
   *   If the call was interrupted while waiting between retries, the {@link InterruptedException} will be added
   *   as a suppressed exception
   */
  public static <V, T extends Throwable> V callWithRetries(Callable<V, T> callable,
                                                           RetryStrategy retryStrategy,
                                                           Predicate<Throwable> isRetryable) throws T {
    return callWithRetries((retryContext) -> callable.call(), retryStrategy, isRetryable);
  }

  /**
   * Executes a {@link CallableWithContext}, retrying the call if it throws something retryable.
   * Passes the attempt count to the function being called.
   *
   * @param callable the callable to run, that takes an integer attempt count
   * @param retryStrategy the retry strategy to use if the callable fails in a retryable way
   * @param isRetryable predicate to determine whether the callable failure is retryable or not
   * @param <V> the type of return value
   * @param <T> the type of throwable
   * @return the return value of the callable
   * @throws T if the callable failed in a way that is not retryable, or the retries were exhausted.
   *   If retries were exhausted, a {@link RetriesExhaustedException} will be added as a suppressed exception.
   *   If the call was interrupted while waiting between retries, the {@link InterruptedException} will be added
   *   as a suppressed exception
   */
  public static <V, T extends Throwable> V callWithRetries(CallableWithContext<V, T> callable,
                                                           RetryStrategy retryStrategy,
                                                           Predicate<Throwable> isRetryable) throws T {

    // Skip the first error log, and at most log once per 30 seconds.
    // This helps debugging errors that persist more than 30 seconds.
    // Use a local variable for this logger to localize the sampling per call.
    Logger errorLogger = Loggers.sampling(
      LOG, LogSamplers.all(LogSamplers.skipFirstN(1), LogSamplers.limitRate(TimeUnit.SECONDS.toMillis(30))));

    int failures = 0;
    long startTime = System.currentTimeMillis();
    while (true) {
      try {
        V v = callable.call(new RetryContext.Builder().withRetryAttempt(failures + 1).build());
        if (failures > 0) {
          LOG.trace("Retry succeeded after {} retries and {} ms.", failures, System.currentTimeMillis() - startTime);
        }
        return v;
      } catch (Throwable t) {
        if (!isRetryable.test(t)) {
          t.addSuppressed(new RetryFailedException("Retry failed. Encountered non retryable exception.", failures + 1));
          throw t;
        }

        long retryTime = retryStrategy.nextRetry(++failures, startTime);
        if (retryTime < 0) {
          String errMsg = String.format("Retries exhausted after %d failures and %d ms.",
                                        failures, System.currentTimeMillis() - startTime);
          LOG.debug(errMsg);
          t.addSuppressed(new RetriesExhaustedException(errMsg, failures + 1));
          throw t;
        }

        LOG.trace("Call failed, retrying again after {} ms.", retryTime, t);
        errorLogger.warn("Call failed with exception, retrying again after {} ms.", retryTime, t);
        try {
          TimeUnit.MILLISECONDS.sleep(retryTime);
        } catch (InterruptedException e) {
          // if we were interrupted while waiting for the next retry, treat it like no retries were attempted
          t.addSuppressed(e);
          Thread.currentThread().interrupt();
          t.addSuppressed(new RetryFailedException("Retry failed. Thread got interrupted.", failures + 1));
          throw t;
        }
      }
    }
  }

  /**
   * Executes a {@link Runnable}, retrying if it throws something retryable.
   *
   * @param runnable the runnable to run
   * @param retryStrategy the retry strategy to use if the callable fails in a retryable way
   * @param isRetryable predicate to determine whether the callable failure is retryable or not
   * @param <T> the type of throwable
   * @throws InterruptedException if the call was interrupted between retries. The last Throwable that triggered the
   *   retry will be added as a suppressed exception.
   * @throws T if the callable failed in a way that is not retryable, or the retries were exhausted.
   *   If retries were exhausted, a {@link RetriesExhaustedException} will be added as a suppressed exception.
   */
  public static <T extends Throwable> void runWithInterruptibleRetries(Runnable<T> runnable,
                                                                       RetryStrategy retryStrategy,
                                                                       Predicate<Throwable> isRetryable)
    throws T, InterruptedException {
    callWithInterruptibleRetries((Callable<Void, T>) () -> {
      runnable.run();
      return null;
    }, retryStrategy, isRetryable);
  }

  /**
   * Executes a {@link Callable}, retrying the call if it throws something retryable.
   *
   * @param callable the callable to run
   * @param retryStrategy the retry strategy to use if the callable fails in a retryable way
   * @param isRetryable predicate to determine whether the callable failure is retryable or not
   * @param <V> the type of return value
   * @param <T> the type of throwable
   * @return the return value of the callable
   * @throws InterruptedException if the call was interrupted between retries. The last Throwable that triggered the
   *   retry will be added as a suppressed exception.
   * @throws T if the callable failed in a way that is not retryable, or the retries were exhausted.
   *   If retries were exhausted, a {@link RetriesExhaustedException} will be added as a suppressed exception.
   */
  public static <V, T extends Throwable> V callWithInterruptibleRetries(Callable<V, T> callable,
                                                                        RetryStrategy retryStrategy,
                                                                        Predicate<Throwable> isRetryable)
    throws T, InterruptedException {
    try {
      return callWithRetries(callable, retryStrategy, isRetryable);
    } catch (Throwable t) {
      for (Throwable suppressed : t.getSuppressed()) {
        if (suppressed instanceof InterruptedException) {
          throw (InterruptedException) suppressed;
        }
      }
      throw t;
    }
  }
}
