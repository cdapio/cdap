/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.common.service;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.retry.RetriesExhaustedException;
import co.cask.cdap.api.retry.RetryableException;
import com.google.common.base.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Utilities to perform logic with retries.
 */
public final class Retries {
  private static final Logger LOG = LoggerFactory.getLogger(Retries.class);
  public static final Predicate<Throwable> ALWAYS_TRUE = new Predicate<Throwable>() {
    @Override
    public boolean apply(Throwable throwable) {
      return true;
    }
  };
  private static final Predicate<Throwable> DEFAULT_PREDICATE = new Predicate<Throwable>() {
    @Override
    public boolean apply(Throwable throwable) {
      return throwable instanceof RetryableException;
    }
  };

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
  public static <V> V supplyWithRetries(final Supplier<V> supplier, RetryStrategy retryStrategy,
                                        Predicate<Throwable> isRetryable) {
    return callWithRetries(new Callable<V, RuntimeException>() {
      @Override
      public V call() throws RuntimeException {
        return supplier.get();
      }
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
   * Executes a {@link Callable}, retrying the call if it throws something retryable.
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

    int failures = 0;
    long startTime = System.currentTimeMillis();
    while (true) {
      try {
        V v = callable.call();
        if (failures > 0) {
          LOG.debug("Retry succeeded after {} retries and {} ms.", failures, System.currentTimeMillis() - startTime);
        }
        return v;
      } catch (Throwable t) {
        if (!isRetryable.apply(t)) {
          throw t;
        }

        long retryTime = retryStrategy.nextRetry(++failures, startTime);
        if (retryTime < 0) {
          String errMsg = String.format("Retries exhausted after %d failures and %d ms.",
                                        failures, System.currentTimeMillis() - startTime);
          LOG.debug(errMsg);
          t.addSuppressed(new RetriesExhaustedException(errMsg));
          throw t;
        }

        LOG.debug("Call failed, retrying again after {} ms.", retryTime, t);
        try {
          TimeUnit.MILLISECONDS.sleep(retryTime);
        } catch (InterruptedException e) {
          // if we were interrupted while waiting for the next retry, treat it like no retries were attempted
          t.addSuppressed(e);
          Thread.currentThread().interrupt();
          throw t;
        }
      }
    }
  }
}
