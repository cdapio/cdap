package com.continuuity.data.operation.executor.remote;

/**
 * A retry strategy is an abstraction over how the remote opex client shuold retry operations after connection
 * failures.
 */
public abstract class RetryStrategy {

  /**
   * Increments the number of failed attempts.
   * @return whether another attempt should be made
   */
  abstract boolean failOnce();

  /**
   * Should be called before re-attempting. This can, for instance
   * inject a sleep time between retries. Default implementation is
   * to do nothing.
   */
  void beforeRetry() {
    // do nothinhg
  }

}
