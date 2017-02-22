/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.distributed;

import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TxConstants;
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

  private int backoffFactor; // factor by which to increase sleep for each retry
  private int maxSleep; // max sleep time. stop retrying when we exceed this
  private int sleep; // current sleep time

  /**
   * @param initialSleep the initial sleep time (before first retry)
   * @param backoff the backoff factor by which sleep time is multiplied
   *                after each retry
   * @param limit the max sleep time. if sleep time reaches this limit, we
   *              stop retrying
   */
  private RetryWithBackoff(int initialSleep, int backoff, int limit) {
    backoffFactor = backoff;
    maxSleep = limit;
    sleep = initialSleep;
  }

  @Override
  public boolean failOnce() {
    return sleep < maxSleep;
  }

  @Override
  public void beforeRetry() {
    LOG.debug("Sleeping " + sleep + " ms before retry.");
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
      initialSleep = TxConstants.Service.DEFAULT_DATA_TX_CLIENT_BACKOFF_INITIAL;
      backoffFactor = TxConstants.Service.DEFAULT_DATA_TX_CLIENT_BACKOFF_FACTOR;
      maxSleep = TxConstants.Service.DEFAULT_DATA_TX_CLIENT_BACKOFF_LIMIT;
    }

    public void configure(Configuration config) {
      initialSleep = config.getInt(TxConstants.Service.CFG_DATA_TX_CLIENT_BACKOFF_INITIAL, initialSleep);
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
