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

import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.common.conf.Constants;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Gathers statistics from a running mapreduce job through its counters and writes the data to the metrics system.
 */
public class MapReduceMetricsWriter {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduceMetricsWriter.class);

  private final Job jobConf;
  private final MetricsContext mapperMetrics;
  private final MetricsContext reducerMetrics;

  public MapReduceMetricsWriter(Job jobConf, BasicMapReduceContext context) {
    this.jobConf = jobConf;
    this.mapperMetrics = context.getProgramMetrics().childContext(Constants.Metrics.Tag.MR_TASK_TYPE,
                                                                  MapReduceMetrics.TaskType.Mapper.getId());
    this.reducerMetrics = context.getProgramMetrics().childContext(Constants.Metrics.Tag.MR_TASK_TYPE,
                                                                   MapReduceMetrics.TaskType.Reducer.getId());
  }

  public void reportStats() throws IOException, InterruptedException {
    Counters jobCounters = jobConf.getCounters();
    reportMapredStats(jobCounters);
  }

  // job level stats from counters built in to mapreduce
  private void reportMapredStats(Counters jobCounters) throws IOException, InterruptedException {
    JobStatus jobStatus = jobConf.getStatus();
    // map stats
    float mapProgress = jobStatus.getMapProgress();
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


    long mapInputRecords = getTaskCounter(jobCounters, TaskCounter.MAP_INPUT_RECORDS);
    long mapOutputRecords = getTaskCounter(jobCounters, TaskCounter.MAP_OUTPUT_RECORDS);
    long mapOutputBytes = getTaskCounter(jobCounters, TaskCounter.MAP_OUTPUT_BYTES);

    mapperMetrics.gauge(MapReduceMetrics.METRIC_COMPLETION, (long) (mapProgress * 100));
    mapperMetrics.gauge(MapReduceMetrics.METRIC_INPUT_RECORDS, mapInputRecords);
    mapperMetrics.gauge(MapReduceMetrics.METRIC_OUTPUT_RECORDS, mapOutputRecords);
    mapperMetrics.gauge(MapReduceMetrics.METRIC_BYTES, mapOutputBytes);
    mapperMetrics.gauge(MapReduceMetrics.METRIC_USED_CONTAINERS, runningMappers);
    mapperMetrics.gauge(MapReduceMetrics.METRIC_USED_MEMORY, runningMappers * memoryPerMapper);

    LOG.trace("Reporting mapper stats: (completion, containers, memory) = ({}, {}, {})",
              (int) (mapProgress * 100), runningMappers, runningMappers * memoryPerMapper);

    // reduce stats
    float reduceProgress = jobStatus.getReduceProgress();
    long reduceInputRecords = getTaskCounter(jobCounters, TaskCounter.REDUCE_INPUT_RECORDS);
    long reduceOutputRecords = getTaskCounter(jobCounters, TaskCounter.REDUCE_OUTPUT_RECORDS);

    reducerMetrics.gauge(MapReduceMetrics.METRIC_COMPLETION, (long) (reduceProgress * 100));
    reducerMetrics.gauge(MapReduceMetrics.METRIC_INPUT_RECORDS, reduceInputRecords);
    reducerMetrics.gauge(MapReduceMetrics.METRIC_OUTPUT_RECORDS, reduceOutputRecords);
    reducerMetrics.gauge(MapReduceMetrics.METRIC_USED_CONTAINERS, runningReducers);
    reducerMetrics.gauge(MapReduceMetrics.METRIC_USED_MEMORY, runningReducers * memoryPerReducer);

    LOG.trace("Reporting reducer stats: (completion, containers, memory) = ({}, {}, {})",
              (int) (reduceProgress * 100), runningReducers, runningReducers * memoryPerReducer);
  }

  private long getTaskCounter(Counters jobCounters, TaskCounter taskCounter) {
    return jobCounters.findCounter(TaskCounter.class.getName(), taskCounter.name()).getValue();
  }
}
