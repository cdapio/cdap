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
package co.cask.cdap.common.metrics;

import java.util.Map;

/**
 * A MetricCollector allows client publish counter metrics.
 */
public interface MetricsCollector {

  /**
   * Log a metric value at the current time.
   * @param metricName Name of the metric.
   * @param value value of the metric.
   */
  void increment(String metricName, long value);

  /**
   * Gauge a metric value at the current time.
   * @param metricName Name of the metric.
   * @param value value of the metric.
   */
  void gauge(String metricName, long value);

  /**
   * Creates child {@link MetricsCollector} that inherits the metrics context from this one and adds extra context
   * information.
   * @param tags tags to add to the child metrics context
   * @return child {@link MetricsCollector}
   */
  MetricsCollector childCollector(Map<String, String> tags);

  /**
   * Convenience method that acts as {@link #childCollector(java.util.Map)} by supplying single tag.
   */
  MetricsCollector childCollector(String tagName, String tagValue);
}
