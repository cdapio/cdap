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
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.TaskCounter;

import java.util.Map;

/**
 * Gathers statistics from a running map task through its counters and writes the data to the metrics system.
 */
public class MapTaskMetricsWriter extends TaskMetricsWriter {

  public MapTaskMetricsWriter(MetricsContext metricsContext, MapContext mapContext) {
    super(metricsContext, mapContext);
  }

  @Override
  public Map<String, TaskCounter> getTaskCounters() {
    return ImmutableMap.of(MapReduceMetrics.METRIC_TASK_INPUT_RECORDS, TaskCounter.MAP_INPUT_RECORDS,
                           MapReduceMetrics.METRIC_TASK_OUTPUT_RECORDS, TaskCounter.MAP_OUTPUT_RECORDS,
                           MapReduceMetrics.METRIC_TASK_BYTES, TaskCounter.MAP_OUTPUT_BYTES);
  }
}
