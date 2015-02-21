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

import java.util.Map;

/**
 * Carries the "raw" emitted metric data point: metric name, context, type, value, etc.
 */
public class MetricValue {
  private final Map<String, String> tags;
  private final String name;
  private final long timestamp;
  private final long value;
  private final MetricType type;

  public MetricValue(Map<String, String> tags, String name, long timestamp, long value, MetricType type) {
    this.tags = tags;
    this.name = name;
    this.timestamp = timestamp;
    this.value = value;
    this.type = type;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public String getName() {
    return name;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public long getValue() {
    return value;
  }

  public MetricType getType() {
    return type;
  }
}
