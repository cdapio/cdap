package com.continuuity.data2.transaction;

/**
 * Retry strategy for failed transactions
 */
public interface RetryStrategy {
  /**
   * Returns the number of milliseconds to wait before retrying the operation.
   *
   * @param reason Reason for transaction failure.
   * @param failureCount Number of times that the request has been failed.
   * @return Number of milliseconds to wait before retrying the operation. Returning {@code 0} means
   *         retry it immediately, while negative means abort the operation.
   */
  long nextRetry(TransactionFailureException reason, int failureCount);
}
