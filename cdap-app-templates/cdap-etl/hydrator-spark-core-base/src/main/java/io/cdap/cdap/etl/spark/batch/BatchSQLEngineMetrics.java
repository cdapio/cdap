/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.batch;

import io.cdap.cdap.api.metrics.Metrics;

import java.util.Map;

/**
 * Metrics instance which prefixes all collected metrics with "pushdown.<engine_name>".
 */
public abstract class BatchSQLEngineMetrics implements Metrics {
  protected final String engineName;
  protected final Metrics metrics;

  protected BatchSQLEngineMetrics(String stageName, Metrics metrics) {
    this.engineName = stageName;
    this.metrics = metrics;
  }

  @Override
  public void count(String metricName, int delta) {
    metrics.count(getMetricName(metricName), delta);
  }

  @Override
  public void countLong(String metricName, long delta) {
    metrics.countLong(getMetricName(metricName), delta);
  }

  @Override
  public void gauge(String metricName, long value) {
    metrics.gauge(getMetricName(metricName), value);
  }

  @Override
  public abstract Metrics child(Map<String, String> tags);

  @Override
  public Map<String, String> getTags() {
    return metrics.getTags();
  }

  protected abstract String getMetricName(String metric);

  /**
   * PipelineSQLEngineMetrics have a format of `user.pushdown.<engine_name>.pipeline.<metric>`
   *
   * These are used to collect SQL Engine metrics related to the pipeline execution (such as record counts)
   */
  public static class PipelineMetrics extends BatchSQLEngineMetrics {
    private static final String METRIC_FORMAT = "pushdown.%s.pipeline.%s";

    public PipelineMetrics(String stageName, Metrics metrics) {
      super(stageName, metrics);
    }

    @Override
    public Metrics child(Map<String, String> tags)  {
      Metrics childMetrics = metrics.child(tags);
      return new PipelineMetrics(engineName, childMetrics);
    }

    protected String getMetricName(String metric) {
      return String.format(METRIC_FORMAT, engineName, metric);
    }
  }

  /**
   * EngineSQLEngineMetrics have a format of `user.pushdown.<engine_name>.engine.<metric>`
   *
   * These are supplied to the SQL Engine metrics for engine-specific metrics (such as resource utilization, cost, etc)
   */
  public static class EngineMetrics extends BatchSQLEngineMetrics {
    private static final String METRIC_FORMAT = "pushdown.%s.engine.%s";

    public EngineMetrics(String stageName, Metrics metrics) {
      super(stageName, metrics);
    }

    @Override
    public Metrics child(Map<String, String> tags)  {
      Metrics childMetrics = metrics.child(tags);
      return new EngineMetrics(engineName, childMetrics);
    }

    protected String getMetricName(String metric) {
      return String.format(METRIC_FORMAT, engineName, metric);
    }
  }
}
