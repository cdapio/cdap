package com.continuuity.data2.transaction;

/**
 * Collection of {@link RetryStrategy}s.
 */
public final class RetryStrategies {
  private RetryStrategies() {}

  /**
   * @param maxRetries max number of retries
   * @param delayInMs delay between retries in milliseconds
   * @return RetryStrategy that retries transaction execution when transaction fails with
   *         {@link TransactionConflictException}
   */
  public static RetryStrategy retryOnConflict(int maxRetries, long delayInMs) {
    return new RetryOnConflictStrategy(maxRetries, delayInMs);
  }

  public static RetryStrategy noRetries() {
    return NoRetryStrategy.INSTANCE;
  }
}
