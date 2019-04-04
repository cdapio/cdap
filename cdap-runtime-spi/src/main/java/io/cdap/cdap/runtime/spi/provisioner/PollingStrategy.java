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

/**
 * Defines how CDAP should poll for cluster status while waiting for a cluster to change state.
 * For example, if it takes at least two minutes to create a cluster,
 * a provisioner can use a strategy where it first waits two minutes, then polls every ten seconds.
 * See {@link PollingStrategies} for some common strategies.
 */
public interface PollingStrategy {

  /**
   * Return the number of milliseconds to wait before the next poll, given the time that polling began, and the number
   * of polls already attempted. Returning 0 or less means the next poll will be tried immediately.
   *
   * @param numPolls the number of times the status was polled already. Starts at 0.
   * @param startTime the timestamp in milliseconds that polling began.
   * @return the number of milliseconds to wait before the next poll
   */
  long nextPoll(int numPolls, long startTime);

}
