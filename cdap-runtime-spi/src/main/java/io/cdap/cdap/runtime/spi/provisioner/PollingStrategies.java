/*
 * Copyright © 2018 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.runtime.spi.provisioner;

import java.util.concurrent.TimeUnit;

/**
 * Some common {@link PollingStrategy PollingStrategies}
 */
public class PollingStrategies {

  /**
   * Return a {@link PollingStrategy} that polls at a fixed interval.
   *
   * @param duration time to wait in between polls
   * @param timeUnit time unit of the wait
   * @return fixed interval polling strategy
   */
  public static PollingStrategy fixedInterval(long duration, TimeUnit timeUnit) {
    return (numPolls, startTime) -> timeUnit.toMillis(duration);
  }

  /**
   * A {@link PollingStrategy} that will wait for a specified amount of time before performing the first poll.
   * Every subsequent poll will be determined by the specified polling strategy.
   *
   * @param strategy polling strategy to use after the first poll
   * @param duration time to wait before the first poll
   * @param timeUnit time unit of the wait
   * @return a polling strategy that has an initial delay added in
   */
  public static PollingStrategy initialDelay(PollingStrategy strategy, long duration, TimeUnit timeUnit) {
    return (numPolls, startTime) -> {
      if (numPolls == 0) {
        return timeUnit.toMillis(duration);
      }
      return strategy.nextPoll(numPolls - 1, startTime);
    };
  }

  /**
   * A {@link PollingStrategy} that will wait for a specified base amount of time plus some random jitter time
   * before performing the first poll. Every subsequent poll will be determined by the specified polling strategy.
   *
   * @param strategy polling strategy to use after the first poll
   * @param baseDuration wait at least this amount of time before the first poll
   * @param jitterDuration maximum jitter time to add to the base duration
   * @param timeUnit time unit of the wait
   * @return a polling strategy that has a random amount of initial delay added in
   */
  public static PollingStrategy initialDelay(PollingStrategy strategy, long baseDuration, long jitterDuration,
                                             TimeUnit timeUnit) {
    return (numPolls, startTime) -> {
      if (numPolls == 0) {
        return timeUnit.toMillis((long) (baseDuration + Math.random() * jitterDuration));
      }
      return strategy.nextPoll(numPolls - 1, startTime);
    };
  }
}
