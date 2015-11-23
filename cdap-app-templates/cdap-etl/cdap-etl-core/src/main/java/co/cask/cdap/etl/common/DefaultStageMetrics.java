/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.StageMetrics;

/**
 * Wrapper around the {@link Metrics} instance from CDAP that prefixes metric names with the ETL context the metric
 * was emitted from.
 */
public class DefaultStageMetrics implements StageMetrics {

  private final Metrics metrics;
  private final String prefix;

  public DefaultStageMetrics(Metrics metrics, String stageName) {
    this.metrics = metrics;
    this.prefix = stageName + ".";
  }

  @Override
  public void count(String metricName, int delta) {
    metrics.count(prefix + metricName, delta);
  }

  @Override
  public void gauge(String metricName, long value) {
    metrics.gauge(prefix + metricName, value);
  }

  @Override
  public void pipelineCount(String metricName, int delta) {
    metrics.count(metricName, delta);
  }

  @Override
  public void pipelineGauge(String metricName, long value) {
    metrics.gauge(metricName, value);
  }
}
