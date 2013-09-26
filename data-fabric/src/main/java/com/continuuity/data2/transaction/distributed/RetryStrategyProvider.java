package com.continuuity.data2.transaction.distributed;

import com.continuuity.common.conf.CConfiguration;

/**
 * A retry strategy provider is used by the tx client to get a new retry strategy for every call.
 */
public interface RetryStrategyProvider {

  /**
   * Provides a new instance of a retry strategy.
   * @return a retry strategy
   */
  RetryStrategy newRetryStrategy();

  /**
   * Configure the strategy.
   * @param config the configuration
   */
  void configure(CConfiguration config);

}
