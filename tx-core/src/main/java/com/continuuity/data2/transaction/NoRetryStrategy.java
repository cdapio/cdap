package com.continuuity.data2.transaction;

/**
 * Does no retries
 */
public class NoRetryStrategy implements RetryStrategy {
  public static final RetryStrategy INSTANCE = new NoRetryStrategy();

  private NoRetryStrategy() {}

  @Override
  public long nextRetry(TransactionFailureException reason, int failureCount) {
    return -1;
  }
}
