/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.logging.save;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs exponential backoff.
 * Modified from Twill's SimpleKafkaConsumer class.
 */
public class ExponentialBackoff {
  private static final Logger LOG = LoggerFactory.getLogger(ExponentialBackoff.class);

  private final long initialBackoff;
  private final long maxBackoff;
  private final BackoffHandler backoffHandler;
  private int failureCount = 0;

  public ExponentialBackoff(long initialBackoff, long maxBackoff, BackoffHandler backoffHandler) {
    this.initialBackoff = initialBackoff;
    this.maxBackoff = maxBackoff;
    this.backoffHandler = backoffHandler;
  }

  /**
   * Performs exponential backoff using {@link ExponentialBackoff.BackoffHandler}
   *
   * @throws InterruptedException if thread is interrupted while handling backoff
   */
  public void backoff() throws InterruptedException {
    failureCount++;
    long multiplier = failureCount > Long.SIZE ? Long.MAX_VALUE : (1L << (failureCount - 1));
    long backoff = Math.min(initialBackoff * multiplier, maxBackoff);
    backoff = backoff < 0 ? maxBackoff : backoff;
    LOG.debug("failureCount = {}, backoff = {}, maxBackoff = {}", failureCount, backoff, maxBackoff);
    backoffHandler.handle(backoff);
  }

  /**
   * Resets the backoff counter so that next backoff starts at beginning
   */
  public void reset() {
    if (failureCount > 0) {
      LOG.debug("failureCount reset to 0");
      failureCount = 0;
    }
  }

  /**
   * Handler to perform backoff
   */
  public interface BackoffHandler {
    void handle(long backoff) throws InterruptedException;
  }
}
