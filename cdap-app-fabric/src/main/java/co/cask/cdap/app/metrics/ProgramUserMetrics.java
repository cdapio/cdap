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

package co.cask.cdap.app.metrics;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;

/**
 * Implementation of {@link Metrics} for user-defined metrics.
 * Metrics will be emitted through {@link MetricsCollectionService}.
 */
public class ProgramUserMetrics implements Metrics {

  private final MetricsCollector metricsCollector;

  public ProgramUserMetrics(MetricsCollector metricsCollector) {
    this.metricsCollector = metricsCollector;
  }

  @Override
  public void count(String metricName, int delta) {
    metricsCollector.increment(metricName, delta);
  }

  @Override
  public void gauge(String metricName, long value) {
    metricsCollector.gauge(metricName, value);
  }
}
