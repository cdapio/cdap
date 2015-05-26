/*
 * Copyright © 2015 Cask Data, Inc.
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

import com.google.common.base.Preconditions;

import java.util.concurrent.TimeUnit;

/**
 * Factory class for creating different types of {@link RetryStrategy}.
 */
public final class RetryStrategies {

  /**
   * @return A {@link RetryStrategy} that doesn't do any retry.
   */
  public static RetryStrategy noRetry() {
    return new RetryStrategy() {
      @Override
      public long nextRetry(int failureCount, long startTime) {
        return -1L;
      }
    };
  }

  /**
   * Creates a {@link RetryStrategy} that retries maximum given number of times, with the actual
   * delay behavior delegated to another {@link RetryStrategy}.
   * @param limit Maximum number of retries allowed.
   * @param strategy When failure count is less than or equal to the limit, this strategy will be called.
   * @return A {@link RetryStrategy}.
   */
  public static RetryStrategy limit(final int limit, final RetryStrategy strategy) {
    Preconditions.checkArgument(limit >= 0, "limit must be >= 0");
    return new RetryStrategy() {
      @Override
      public long nextRetry(int failureCount, long startTime) {
        return (failureCount <= limit) ? strategy.nextRetry(failureCount, startTime) : -1L;
      }
    };
  }

  /**
   * Creates a {@link RetryStrategy} that imposes a fix delay between each retries.
   * @param delay delay time
   * @param delayUnit {@link TimeUnit} for the delay.
   * @return A {@link RetryStrategy}.
   */
  public static RetryStrategy fixDelay(final long delay, final TimeUnit delayUnit) {
    Preconditions.checkArgument(delay >= 0, "delay must be >= 0");
    return new RetryStrategy() {
      @Override
      public long nextRetry(int failureCount, long startTime) {
        return TimeUnit.MILLISECONDS.convert(delay, delayUnit);
      }
    };
  }

  /**
   * Creates a {@link RetryStrategy} that will increase delay exponentially between each retries.
   * @param baseDelay delay to start with.
   * @param maxDelay cap of the delay.
   * @param delayUnit {@link TimeUnit} for the delays.
   * @return A {@link RetryStrategy}.
   */
  public static RetryStrategy exponentialDelay(final long baseDelay, final long maxDelay, final TimeUnit delayUnit) {
    Preconditions.checkArgument(baseDelay >= 0, "base delay must be >= 0");
    Preconditions.checkArgument(maxDelay >= 0, "max delay must be >= 0");
    return new RetryStrategy() {
      @Override
      public long nextRetry(int failureCount, long startTime) {
        long power = failureCount > Long.SIZE ? Long.MAX_VALUE : (1L << (failureCount - 1));
        long delay = Math.min(baseDelay * power, maxDelay);
        delay = delay < 0 ? maxDelay : delay;
        return TimeUnit.MILLISECONDS.convert(delay, delayUnit);
      }
    };
  }

  /**
   * Creates a {@link RetryStrategy} that will retry until maximum amount of time has been passed since the request,
   * with the actual delay behavior delegated to another {@link RetryStrategy}.
   * @param maxElapseTime Maximum amount of time until giving up retry.
   * @param timeUnit {@link TimeUnit} for the max elapse time.
   * @param strategy When time elapsed is less than or equal to the limit, this strategy will be called.
   * @return A {@link RetryStrategy}.
   */
  public static RetryStrategy timeLimit(long maxElapseTime, TimeUnit timeUnit, final RetryStrategy strategy) {
    Preconditions.checkArgument(maxElapseTime >= 0, "max elapse time must be >= 0");
    final long maxElapseMs = TimeUnit.MILLISECONDS.convert(maxElapseTime, timeUnit);
    return new RetryStrategy() {
      @Override
      public long nextRetry(int failureCount, long startTime) {
        long elapseTime = System.currentTimeMillis() - startTime;
        return elapseTime <= maxElapseMs ? strategy.nextRetry(failureCount, startTime) : -1L;
      }
    };
  }

  private RetryStrategies() {
  }
}
