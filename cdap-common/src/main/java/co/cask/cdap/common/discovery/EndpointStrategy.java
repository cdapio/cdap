/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.common.discovery;

import org.apache.twill.discovery.Discoverable;

import java.util.concurrent.TimeUnit;

/**
 * This class helps picking up an endpoint from a list of Discoverable.
 */
public interface EndpointStrategy {

  /**
   * Picks a {@link Discoverable} using its strategy.
   * @return A {@link Discoverable} based on the strategy or {@code null} if no endpoint can be found.
   */
  Discoverable pick();

  /**
   * Waits if necessary for at most the given time to pick a {@link Discoverable}.
   *
   * @param timeout the maximum time to wait
   * @param timeoutUnit the time unit of the timeout argument
   * @return A {@link Discoverable} based on the strategy or {@code null} if no endpoint can be found after
   *         the given timeout passed.
   */
  Discoverable pick(long timeout, TimeUnit timeoutUnit);
}
