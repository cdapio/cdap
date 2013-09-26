package com.continuuity.data2.transaction.distributed;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;

/**
 * A retry strategy that makes N attempts and then gives up. This does
 * not do anything before the re-attempt - extend this class to add a
 * sleep or similar.
 */
public class RetryNTimes extends RetryStrategy {

  int attempts = 0;
  int limit;

  /**
   * @param maxAttempts the number of attempts after which to stop
   */
  protected RetryNTimes(int maxAttempts) {
    limit = maxAttempts;
  }

  @Override
  boolean failOnce() {
    ++attempts;
    return attempts < limit;
  }

  /**
   * A retry strategy provider for this strategy.
   */
  public static class Provider implements RetryStrategyProvider {

    int nTimes;

    public Provider() {
      this.nTimes = Constants.Transaction.Service.DEFAULT_DATA_TX_CLIENT_ATTEMPTS;
    }

    @Override
    public void configure(CConfiguration config) {
      nTimes = config.getInt(Constants.Transaction.Service.CFG_DATA_TX_CLIENT_ATTEMPTS, nTimes);
    }

    @Override
    public RetryStrategy newRetryStrategy() {
      return new RetryNTimes(nTimes);
    }

    @Override
    public String toString() {
      return nTimes + " attempts without delay";
    }
  }
}
