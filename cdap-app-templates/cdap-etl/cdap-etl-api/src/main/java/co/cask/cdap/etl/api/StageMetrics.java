/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.etl.api;

import co.cask.cdap.api.metrics.Metrics;

/**
 * The interface exposes method for emitting user metrics from ETL stage.
 */
public interface StageMetrics extends Metrics {

  /**
   * Increases the value of the specific metric by delta. Metrics name will be prefixed by the
   * stage id, hence it will be aggregated for the current stage.
   *
   * @param metricName Name of the counter. Use alphanumeric characters in metric names.
   * @param delta The value to increase by.
   */
  @Override
  void count(String metricName, int delta);

  /**
   * Sets the specific metric to the provided value. Metrics name will be prefixed by the
   * stage id, hence it will be aggregated for the current stage.
   *
   * @param metricName Name of the counter. Use alphanumeric characters in metric names.
   * @param value The value to be set.
   */
  @Override
  void gauge(String metricName, long value);

  /**
   * Increases the value of the specific metric by delta. Metrics emitted will be aggregated
   * for the whole ETL pipeline.
   *
   * @param metricName Name of the counter. Use alphanumeric characters in metric names.
   * @param delta The value to increase by.
   */
  void pipelineCount(String metricName, int delta);

  /**
   * Sets the specific metric to the provided value. Metrics emitted will be aggregated
   * for the whole ETL pipeline.
   *
   * @param metricName Name of the counter. Use alphanumeric characters in metric names.
   * @param value The value to be set.
   */
  void pipelineGauge(String metricName, long value);
}
