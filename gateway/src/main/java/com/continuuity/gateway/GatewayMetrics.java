/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.gateway;

import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;

/**
 * This collects basic metrics like number of requests. In some way this this is hack, because we need to store metrics
 * in persistent store by using generic metrics framework. Due to time constraints (we wanted to get at least some
 * on the usage patterns of about to be released Developer Sandbox) this approach is applied.
 */
public class GatewayMetrics {
  private Map<String, Long> metrics = Maps.newConcurrentMap();

  /**
   * Increments counter. If counter doesn't exist creates new one.
   *
   * @param counterName counter to increment
   * @param delta       value to increment by
   */
  public void count(String counterName, long delta) {
    Long existingValue = metrics.get(counterName);
    if (existingValue == null) {
      metrics.put(counterName, delta);
    } else {
      metrics.put(counterName, existingValue + delta);
    }
  }

  public Map<String, Long> getMetrics() {
    return Collections.unmodifiableMap(metrics);
  }
}
