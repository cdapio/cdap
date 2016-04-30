/*
 * Copyright © 2016 Cask Data, Inc.
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
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.conf.Constants;

/**
 * Implementation of {@link Metrics}, context used by programs, if they are a part of a workflow.
 * we emit metrics in two context, program context and workflow context.
 * Metrics will be emitted through {@link MetricsCollectionService}
 */
public class WorkflowMetrics implements Metrics {
  private final MetricsContext programMetricsContext;
  private final MetricsContext workflowMetricsContext;

  public WorkflowMetrics(MetricsContext programMetricsContext, MetricsContext workflowMetricsContext) {
    this.programMetricsContext = programMetricsContext.childContext(Constants.Metrics.Tag.SCOPE, "user");
    this.workflowMetricsContext = workflowMetricsContext.childContext(Constants.Metrics.Tag.SCOPE, "user");
  }

  @Override
  public void count(String metricName, int delta) {
    programMetricsContext.increment(metricName, delta);
    workflowMetricsContext.increment(metricName, delta);
  }

  @Override
  public void gauge(String metricName, long value) {
    programMetricsContext.gauge(metricName, value);
    workflowMetricsContext.gauge(metricName, value);
  }
}
