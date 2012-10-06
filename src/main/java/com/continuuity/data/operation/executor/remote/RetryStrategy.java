package com.continuuity.data.operation.executor.remote;

public abstract class RetryStrategy {

  /**
   * increments the number of failed attempts
   * @return whether another attempt should be made
   */
  abstract boolean failOnce();

  /**
   * should be called before re-attempting. This can, for instance
   * inject a sleep time between retries. Default implementation is
   * to do nothing.
   */
  void beforeRetry() {
    // do nothinhg
  }

}
