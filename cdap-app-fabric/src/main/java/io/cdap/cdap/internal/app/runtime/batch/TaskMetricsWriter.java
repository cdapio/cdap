/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.app.metrics.MapReduceMetrics;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.util.Map;

/**
 * Base class for gathering statistics from a running map/reduce task through its counters and for writing the data to
 * the metrics system.
 */
public abstract class TaskMetricsWriter {
  private final MetricsContext metricsContext;
  private final TaskInputOutputContext taskContext;

  TaskMetricsWriter(MetricsContext metricsContext, TaskInputOutputContext taskContext) {
    this.metricsContext = metricsContext;
    this.taskContext = taskContext;
  }

  public void reportMetrics() {
    metricsContext.gauge(MapReduceMetrics.METRIC_TASK_COMPLETION, (long) (taskContext.getProgress() * 100));
    for (Map.Entry<String, TaskCounter> counterEntry : getTaskCounters().entrySet()) {
      metricsContext.gauge(counterEntry.getKey(), getTaskCounter(counterEntry.getValue()));
    }
  }

  private long getTaskCounter(TaskCounter taskCounter) {
    return taskContext.getCounter(TaskCounter.class.getName(), taskCounter.name()).getValue();
  }

  protected abstract Map<String, TaskCounter> getTaskCounters();
}
