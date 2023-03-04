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

package io.cdap.cdap.api.metrics;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;

/**
 * An Interface that provides methods to publish metrics to different sinks.
 */
public interface MetricsPublisher extends Closeable {

  /**
   * Function that publishes a collection of metrics with a common set of context tags.
   *
   * @param metrics List of {@link MetricValue} to be published.
   * @param tags Map of tags that specify the context of the metrics that are published.
   * @throws Exception when there are error during publishing.
   */
  void publish(Collection<MetricValue> metrics, Map<String, String> tags) throws Exception;

  /**
   * Function that publishes a collection of metric values that already have tags and timestamp
   * attached.
   *
   * @param metrics Collection of {@link MetricValues} to be published.
   * @throws Exception when there are error during publishing.
   */
  void publish(Collection<MetricValues> metrics) throws Exception;

  /**
   * Function to initialize resources used by publisher on calling {@code publish}
   */
  void initialize();
}
