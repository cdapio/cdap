package com.continuuity.data2.transaction;

/**
 * Retries transaction execution when transaction fails with {@link TransactionConflictException}.
 */
public class RetryOnConflictStrategy implements RetryStrategy {
  private final int maxRetries;
  private final long retryDelay;

  public RetryOnConflictStrategy(int maxRetries, long retryDelay) {
    this.maxRetries = maxRetries;
    this.retryDelay = retryDelay;
  }

  @Override
  public long nextRetry(TransactionFailureException reason, int failureCount) {
    if (reason instanceof TransactionConflictException) {
      return failureCount > maxRetries ? -1 : retryDelay;
    } else {
      return -1;
    }
  }
}
