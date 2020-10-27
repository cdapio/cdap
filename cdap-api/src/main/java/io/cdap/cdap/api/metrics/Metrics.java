/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.api.metrics;

import java.util.Map;

/**
 * Defines a way to collect user-defined metrics.
 * To use it, just add a Metrics field in a CDAP application element and start using it.
 */
public interface Metrics {
  /**
   * Increases the value of the specific metric by delta.
   * @param metricName Name of the counter. Use alphanumeric characters in metric names.
   * @param delta The value to increase by.
   */
  void count(String metricName, int delta);

  /**
   * Sets the specific metric to the provided value.
   * @param metricName Name of the counter. Use alphanumeric characters in metric names.
   * @param value The value to be set.
   */
  void gauge(String metricName, long value);

  /**
   * Get the child {@link Metrics} by adding specified tags to the current metrics context.
   * @param tags to be added to the current metrics context
   * @return an instance of {@link Metrics} which can be used to collect metrics with new child context.
   * @throws IllegalArgumentException if any of the supplied tag name already exists in the current metrics context
   */
  Metrics child(Map<String, String> tags);

  /**
   * @return the tags that identifies the metrics context for this {@link Metrics} instance.
   */
  Map<String, String> getTags();
}

