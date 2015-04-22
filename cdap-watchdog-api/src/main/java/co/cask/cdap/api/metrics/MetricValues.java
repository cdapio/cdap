/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.api.metrics;

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.Map;

/**
 * Carries the "raw" emitted metric data points: context, timestamp, collection of
 * {@link co.cask.cdap.api.metrics.MetricValue}
 */
public class MetricValues {
  private final Map<String, String> tags;
  private final Collection<MetricValue> metrics;

  /**
   * Timestamp in seconds.
   */
  private final long timestamp;

  public MetricValues(Map<String, String> tags, long timestamp, Collection<MetricValue> metrics) {
    this.tags = tags;
    this.timestamp = timestamp;
    this.metrics = metrics;
  }

  public MetricValues(Map<String, String> tags, String name, long timestamp, long value, MetricType type) {
    this.tags = tags;
    this.timestamp = timestamp;
    this.metrics = ImmutableList.of(new MetricValue(name, type, value));
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public Collection<MetricValue> getMetrics() {
    return metrics;
  }

  public long getTimestamp() {
    return timestamp;
  }

}
