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

package io.cdap.cdap.etl.mock.common;

import com.google.common.collect.Maps;
import io.cdap.cdap.api.metrics.Metrics;

import java.util.Collections;
import java.util.Map;

/**
 * Mock metrics for unit tests.
 */
public class MockMetrics implements Metrics {
  private final Map<String, Long> gauges = Maps.newHashMap();
  private final Map<String, Long> counts = Maps.newHashMap();

  @Override
  public void count(String s, int i) {
    if (counts.containsKey(s)) {
      counts.put(s, counts.get(s) + i);
    } else {
      counts.put(s, (long) i);
    }
  }

  @Override
  public void countLong(String s, long l) {
    if (counts.containsKey(s)) {
      counts.put(s, counts.get(s) + l);
    } else {
      counts.put(s, l);
    }
  }

  @Override
  public void gauge(String s, long l) {
    gauges.put(s, l);
  }

  @Override
  public void event(String s, long l) {
    // TODO
  }

  @Override
  public Metrics child(Map<String, String> tags) {
    return this;
  }

  @Override
  public Map<String, String> getTags() {
    return Collections.emptyMap();
  }

  public long getCount(String metric) {
    Long count = counts.get(metric);
    return count == null ? 0 : count;
  }

  public long getGauge(String metric) {
    Long val = gauges.get(metric);
    return val == null ? 0 : val;
  }

  public void clearMetrics() {
    counts.clear();
    gauges.clear();
  }
}
