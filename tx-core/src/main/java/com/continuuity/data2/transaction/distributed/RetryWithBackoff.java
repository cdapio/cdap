package com.continuuity.data2.transaction.distributed;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.TxConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A retry strategy that makes N attempts and then gives up. This does
 * not do anything before the re-attempt - extend this class to add a
 * sleep or similar.
 */
public class RetryWithBackoff extends RetryStrategy {

  private static final Logger LOG =
      LoggerFactory.getLogger(RetryWithBackoff.class);

  int initialSleep; // initial sleep time
  int backoffFactor; // factor by which to increase sleep for each retry
  int maxSleep; // max sleep time. stop retrying when we exceed this
  int sleep; // current sleep time

  /**
   * @param initial the initial sleep time (before first retry)
   * @param backoff the backoff factor by which sleep time is multiplied
   *                after each retry
   * @param limit the max sleep time. if sleep time reaches this limit, we
   *              stop retrying
   */
  protected RetryWithBackoff(int initial, int backoff, int limit) {
    initialSleep = initial;
    backoffFactor = backoff;
    maxSleep = limit;
    sleep = initialSleep;
  }

  @Override
  boolean failOnce() {
    return sleep < maxSleep;
  }

  @Override
  void beforeRetry() {
    LOG.info("Sleeping " + sleep + " ms before retry.");
    long current = System.currentTimeMillis();
    long end = current + sleep;
    while (current < end) {
      try {
        Thread.sleep(end - current);
      } catch (InterruptedException e) {
        // do nothing
      }
      current = System.currentTimeMillis();
    }
    sleep = sleep * backoffFactor;
  }

  /**
   * A provider for this retry strategy.
   */
  public static class Provider implements RetryStrategyProvider {

    int initialSleep; // initial sleep time
    int backoffFactor; // factor by which to increase sleep for each retry
    int maxSleep; // max sleep time. stop retrying when we exceed this

    public Provider() {
      initialSleep = TxConstants.Service.DEFAULT_DATA_TX_CLIENT_BACKOFF_INIITIAL;
      backoffFactor = TxConstants.Service.DEFAULT_DATA_TX_CLIENT_BACKOFF_FACTOR;
      maxSleep = TxConstants.Service.DEFAULT_DATA_TX_CLIENT_BACKOFF_LIMIT;
    }

    public void configure(CConfiguration config) {
      initialSleep = config.getInt(TxConstants.Service.CFG_DATA_TX_CLIENT_BACKOFF_INIITIAL, initialSleep);
      backoffFactor = config.getInt(TxConstants.Service.CFG_DATA_TX_CLIENT_BACKOFF_FACTOR, backoffFactor);
      maxSleep = config.getInt(TxConstants.Service.CFG_DATA_TX_CLIENT_BACKOFF_LIMIT, maxSleep);
    }

    @Override
    public RetryStrategy newRetryStrategy() {
      return new RetryWithBackoff(initialSleep, backoffFactor, maxSleep);
    }

    @Override
    public String toString() {
      return "sleep " + initialSleep + " ms with back off factor " +
          backoffFactor + " and limit " + maxSleep + " ms";
    }
  }
}
