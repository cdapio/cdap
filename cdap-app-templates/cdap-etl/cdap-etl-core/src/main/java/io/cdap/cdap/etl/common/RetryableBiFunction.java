/*
 * Copyright © 2023 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.etl.common;

import com.google.common.base.Stopwatch;
import io.cdap.cdap.api.retry.RetryableException;

import java.util.concurrent.TimeUnit;

/**
 * {@link RetryableBiFunction} is a retryable function which takes two arguments, returns a result, and may throw.
 * @param <T> The first argument type
 * @param <U> The second argument type
 * @param <R> The return type
 */
public abstract class RetryableBiFunction<T, U, R> {
  private static final long TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(600);
  private static final long RETRY_BASE_DELAY_MILLIS = 200L;
  private static final long RETRY_MAX_DELAY_MILLIS = TimeUnit.SECONDS.toMillis(5);
  private static final double RETRY_DELAY_MULTIPLIER = 1.2d;
  private static final double RETRY_RANDOMIZE_FACTOR = 0.1d;

  /**
   * Executes this BiFunction with retries.
   * @param funcName The function being executed
   */
  public R applyWithRetries(String funcName, T t, U u) {
    long delay = RETRY_BASE_DELAY_MILLIS;
    double minMultiplier =
      RETRY_DELAY_MULTIPLIER - RETRY_DELAY_MULTIPLIER * RETRY_RANDOMIZE_FACTOR;
    double maxMultiplier =
      RETRY_DELAY_MULTIPLIER + RETRY_DELAY_MULTIPLIER * RETRY_RANDOMIZE_FACTOR;
    Stopwatch stopWatch = new Stopwatch().start();
    try {
      while (stopWatch.elapsedTime(TimeUnit.MILLISECONDS) < TIMEOUT_MILLIS) {
        try {
          return apply(t, u);
        } catch (RetryableException e) {
          TimeUnit.MILLISECONDS.sleep(delay);
          delay =
            (long) (delay * (minMultiplier + Math.random() * (maxMultiplier - minMultiplier + 1)));
          delay = Math.min(delay, RETRY_MAX_DELAY_MILLIS);
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(String.format("Thread interrupted while trying to execute retryable bi-function " +
                                                 "'%s' with args '%s',' '%s'", funcName, t, u), e);
    } catch (Throwable throwable) {
      throw new RuntimeException(String.format("Failed to execute retryable bi-function '%s' with args '%s',' '%s'",
                                               funcName, t, u), throwable);
    }
    throw new IllegalStateException(String.format("Timed out trying to execute retryable function '%s' with args " +
                                                    "'%s', '%s'", funcName, t, u));
  }

  /**
   * The function to retry.
   */
  protected abstract R apply(T t, U u) throws Throwable;
}
