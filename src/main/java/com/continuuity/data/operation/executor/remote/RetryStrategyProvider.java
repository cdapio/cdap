package com.continuuity.data.operation.executor.remote;

import com.continuuity.common.conf.CConfiguration;

public interface RetryStrategyProvider {

  /**
   * provides a new instance of a retry strategy
   * @return a retry strategy
   */
  RetryStrategy newRetryStrategy();

  /**
   * configure the strategy
   * @param config the configuration
   */
  void configure(CConfiguration config);

}
