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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricTags;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.metrics.collect.MapReduceCounterCollectionService;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Gathers statistics from a running mapreduce job through its counters and writes the data to the metrics system.
 */
public class MapReduceMetricsWriter {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduceMetricsWriter.class);
  private static final String METRIC_INPUT_RECORDS = "process.entries.in";
  private static final String METRIC_OUTPUT_RECORDS = "process.entries.out";
  private static final String METRIC_BYTES = "process.bytes";
  private static final String METRIC_COMPLETION = "process.completion";
  private static final String METRIC_USED_CONTAINERS = "resources.used.containers";
  private static final String METRIC_USED_MEMORY = "resources.used.memory";

  private final Job jobConf;
  private final BasicMapReduceContext context;
  private final MetricsCollector mapperMetrics;
  private final MetricsCollector reducerMetrics;

  public MapReduceMetricsWriter(Job jobConf, BasicMapReduceContext context) {
    this.jobConf = jobConf;
    this.context = context;
    this.mapperMetrics = context.getProgramMetrics().childCollector(MetricTags.MR_TASK_TYPE.getCodeName(),
                                                                    MapReduceMetrics.TaskType.Mapper.getId());
    this.reducerMetrics = context.getProgramMetrics().childCollector(MetricTags.MR_TASK_TYPE.getCodeName(),
                                                                    MapReduceMetrics.TaskType.Reducer.getId());
  }

  public void reportStats() throws IOException, InterruptedException {
    reportMapredStats();
    reportSystemStats();
  }

  // job level stats from counters built in to mapreduce
  private void reportMapredStats() throws IOException, InterruptedException {
    // map stats
    float mapProgress = jobConf.getStatus().getMapProgress();
    int runningMappers = 0;
    int runningReducers = 0;
    for (TaskReport tr : jobConf.getTaskReports(TaskType.MAP)) {
      runningMappers += tr.getRunningTaskAttemptIds().size();
    }
    for (TaskReport tr : jobConf.getTaskReports(TaskType.REDUCE)) {
      runningReducers += tr.getRunningTaskAttemptIds().size();
    }
    int memoryPerMapper = jobConf.getConfiguration().getInt(Job.MAP_MEMORY_MB, Job.DEFAULT_MAP_MEMORY_MB);
    int memoryPerReducer = jobConf.getConfiguration().getInt(Job.REDUCE_MEMORY_MB, Job.DEFAULT_REDUCE_MEMORY_MB);

    long mapInputRecords = getTaskCounter(TaskCounter.MAP_INPUT_RECORDS);
    long mapOutputRecords = getTaskCounter(TaskCounter.MAP_OUTPUT_RECORDS);
    long mapOutputBytes = getTaskCounter(TaskCounter.MAP_OUTPUT_BYTES);

    mapperMetrics.gauge(METRIC_COMPLETION, (long) (mapProgress * 100));
    mapperMetrics.gauge(METRIC_INPUT_RECORDS, mapInputRecords);
    mapperMetrics.gauge(METRIC_OUTPUT_RECORDS, mapOutputRecords);
    mapperMetrics.gauge(METRIC_BYTES, mapOutputBytes);
    mapperMetrics.gauge(METRIC_USED_CONTAINERS, runningMappers);
    mapperMetrics.gauge(METRIC_USED_MEMORY, runningMappers * memoryPerMapper);

    LOG.trace("Reporting mapper stats: (completion, ins, outs, bytes, containers, memory) = ({}, {}, {}, {}, {}, {})",
              (int) (mapProgress * 100), mapInputRecords, mapOutputRecords, mapOutputBytes, runningMappers,
              runningMappers * memoryPerMapper);

    // reduce stats
    float reduceProgress = jobConf.getStatus().getReduceProgress();
    long reduceInputRecords = getTaskCounter(TaskCounter.REDUCE_INPUT_RECORDS);
    long reduceOutputRecords = getTaskCounter(TaskCounter.REDUCE_OUTPUT_RECORDS);

    reducerMetrics.gauge(METRIC_COMPLETION, (long) (reduceProgress * 100));
    reducerMetrics.gauge(METRIC_INPUT_RECORDS, reduceInputRecords);
    reducerMetrics.gauge(METRIC_OUTPUT_RECORDS, reduceOutputRecords);
    reducerMetrics.gauge(METRIC_USED_CONTAINERS, runningReducers);
    reducerMetrics.gauge(METRIC_USED_MEMORY, runningReducers * memoryPerReducer);

    LOG.trace("Reporting reducer stats: (completion, ins, outs, containers, memory) = ({}, {}, {}, {}, {})",
              (int) (reduceProgress * 100), reduceInputRecords, reduceOutputRecords, runningReducers,
              runningReducers * memoryPerReducer);
  }

  // report system stats coming from user metrics or dataset operations
  private void reportSystemStats() throws IOException, InterruptedException {
    Counters counters = jobConf.getCounters();
    for (String group : counters.getGroupNames()) {
      if (group.startsWith("cdap.")) {

        Map<String, String> tags = MapReduceCounterCollectionService.parseTags(group);
        // todo: use some caching?
        MetricsCollector collector = context.getProgramMetrics().childCollector(tags);

        // Note: all mapreduce metrics are reported as gauges due to how mapreduce counters work;
        //       we may later emit metrics right from the tasks into the metrics system to overcome this limitation
        for (Counter counter : counters.getGroup(group)) {
          collector.gauge(counter.getName(), counter.getValue());
        }
      }
    }
  }

  private long getTaskCounter(TaskCounter taskCounter) throws IOException, InterruptedException {
    return jobConf.getCounters().findCounter(TaskCounter.class.getName(), taskCounter.name()).getValue();
  }

}
