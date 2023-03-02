/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.metrics.publisher;

import io.cdap.cdap.api.metrics.MetricValue;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsPublisher;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Abstract base class for various {@link MetricsPublisher}s
 */
abstract class AbstractMetricsPublisher implements MetricsPublisher {

  /**
   * Function that adds timestamp and tags to collection of {@link MetricValue} to convert it into a
   * Collection of {@link MetricValues} and calls the overloaded publish method with the Collection
   * of {@link MetricValues}.
   *
   * @param metrics List of {@link MetricValue} to be published.
   * @param tags Map of tags that specify the context of the metrics that are published.
   * @throws Exception When the publisher isn't initialized or an exception is raised during the
   *     publishing process.
   */
  @Override
  public void publish(Collection<MetricValue> metrics, Map<String, String> tags) throws Exception {
    Collection<MetricValues> metricValues = new ArrayList<>();
    for (MetricValue metric : metrics) {
      long now = System.currentTimeMillis();
      metricValues.add(new MetricValues(tags, metric.getName(),
          TimeUnit.MILLISECONDS.toSeconds(now),
          metric.getValue(), metric.getType()));
    }
    this.publish(metricValues);
  }
}
