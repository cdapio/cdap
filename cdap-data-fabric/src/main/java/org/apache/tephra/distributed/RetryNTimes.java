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

/**
 * A retry strategy that makes N attempts and then gives up. This does
 * not do anything before the re-attempt - extend this class to add a
 * sleep or similar.
 */
public class RetryNTimes extends RetryStrategy {

  private int attempts = 0;
  private int limit;

  /**
   * @param maxAttempts the number of attempts after which to stop
   */
  private RetryNTimes(int maxAttempts) {
    limit = maxAttempts;
  }

  @Override
  public boolean failOnce() {
    ++attempts;
    return attempts < limit;
  }

  /**
   * A retry strategy provider for this strategy.
   */
  public static class Provider implements RetryStrategyProvider {

    int nTimes;

    public Provider() {
      this.nTimes = TxConstants.Service.DEFAULT_DATA_TX_CLIENT_ATTEMPTS;
    }

    @Override
    public void configure(Configuration config) {
      nTimes = config.getInt(TxConstants.Service.CFG_DATA_TX_CLIENT_ATTEMPTS, nTimes);
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
