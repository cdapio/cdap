package com.continuuity.data.operation.executor.remote;

public interface RetryStrategyProvider {

  /**
   * provides a new instance of a retry strategy
   * @return a retry strategy
   */
  RetryStrategy newRetryStrategy();

}
