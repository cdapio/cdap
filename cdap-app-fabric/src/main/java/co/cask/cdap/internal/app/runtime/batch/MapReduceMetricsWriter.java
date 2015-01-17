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

import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.metrics.transport.MetricType;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
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
  private final Table<MetricsScope, String, Integer> previousMapStats;
  private final Table<MetricsScope, String, Integer> previousReduceStats;
  private final Table<MetricsScope, String, Integer> previousDatasetStats;

  public MapReduceMetricsWriter(Job jobConf, BasicMapReduceContext context) {
    this.jobConf = jobConf;
    this.context = context;
    this.previousMapStats = HashBasedTable.create();
    this.previousReduceStats = HashBasedTable.create();
    this.previousDatasetStats = HashBasedTable.create();
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

    // mapred counters are running counters whereas our metrics timeseries and aggregates make more
    // sense as incremental numbers.  So we want to subtract the current counter value from the previous before
    // emitting to the metrics system.
    long mapInputRecords = getTaskCounter(TaskCounter.MAP_INPUT_RECORDS);
    long mapOutputRecords = getTaskCounter(TaskCounter.MAP_OUTPUT_RECORDS);
    long mapOutputBytes = getTaskCounter(TaskCounter.MAP_OUTPUT_BYTES);

    context.getSystemMapperMetrics().gauge(METRIC_COMPLETION, (long) (mapProgress * 100));
    context.getSystemMapperMetrics().gauge(METRIC_INPUT_RECORDS, mapInputRecords);
    context.getSystemMapperMetrics().gauge(METRIC_OUTPUT_RECORDS, mapOutputRecords);
    context.getSystemMapperMetrics().gauge(METRIC_BYTES, mapOutputBytes);
    context.getSystemMapperMetrics().gauge(METRIC_USED_CONTAINERS, runningMappers);
    context.getSystemMapperMetrics().gauge(METRIC_USED_MEMORY, runningMappers * memoryPerMapper);

    LOG.trace("Reporting mapper stats: (completion, ins, outs, bytes, containers, memory) = ({}, {}, {}, {}, {}, {})",
              (int) (mapProgress * 100), mapInputRecords, mapOutputRecords, mapOutputBytes, runningMappers,
              runningMappers * memoryPerMapper);

    // reduce stats
    float reduceProgress = jobConf.getStatus().getReduceProgress();
    long reduceInputRecords = getTaskCounter(TaskCounter.REDUCE_INPUT_RECORDS);
    long reduceOutputRecords = getTaskCounter(TaskCounter.REDUCE_OUTPUT_RECORDS);

    context.getSystemReducerMetrics().gauge(METRIC_COMPLETION, (long) (reduceProgress * 100));
    context.getSystemReducerMetrics().gauge(METRIC_INPUT_RECORDS, reduceInputRecords);
    context.getSystemReducerMetrics().gauge(METRIC_OUTPUT_RECORDS, reduceOutputRecords);
    context.getSystemReducerMetrics().gauge(METRIC_USED_CONTAINERS, runningReducers);
    context.getSystemReducerMetrics().gauge(METRIC_USED_MEMORY, runningReducers * memoryPerReducer);

    LOG.trace("Reporting reducer stats: (completion, ins, outs, containers, memory) = ({}, {}, {}, {}, {})",
              (int) (reduceProgress * 100), reduceInputRecords, reduceOutputRecords, runningReducers,
              runningReducers * memoryPerReducer);
  }

  // report system stats coming from user metrics or dataset operations
  private void reportSystemStats() throws IOException, InterruptedException {
    Counters counters = jobConf.getCounters();
    for (String group : counters.getGroupNames()) {
      if (group.startsWith("cdap.")) {
        String[] parts = group.split("\\.");
        MetricType metricType = MetricType.valueOf(parts[parts.length - 2]);
        String scopePart = parts[parts.length - 1];
        // last one should be scope
        MetricsScope scope;
        try {
          scope = MetricsScope.valueOf(scopePart);
        } catch (IllegalArgumentException e) {
          // SHOULD NEVER happen, simply skip if happens
          continue;
        }

        //TODO: Refactor to support any context
        String programPart = parts[1];
        if (programPart.equals("mapper")) {
          reportSystemStats(counters.getGroup(group), context.getSystemMapperMetrics(scope), scope,
                            previousMapStats, metricType);
        } else if (programPart.equals("reducer")) {
          reportSystemStats(counters.getGroup(group), context.getSystemReducerMetrics(scope), scope,
                            previousReduceStats, metricType);
        }
      }
    }
  }

  // convenience function to set the table value to the given value and return the difference between the given value
  // and the previous table value, where the previous value is 0 if there was no entry in the table to begin with.
  private int calcDiffAndSetTableValue(Table<MetricsScope, String, Integer> table,
                                       MetricsScope scope, String key, long value) {
    Integer previous = table.get(scope, key);
    previous = (previous == null) ? 0 : previous;
    table.put(scope, key, (int) value);
    return (int) value - previous;
  }

  private void reportSystemStats(Iterable<Counter> counters, MetricsCollector collector, MetricsScope scope,
                                 Table<MetricsScope, String, Integer> previousStats,  MetricType type) {
    if (type == MetricType.GAUGE) {
      reportGaugeSystemStats(counters, collector);
    } else {
      reportIncrementSystemStats(counters, collector, scope, previousStats);
    }
  }


  private void reportIncrementSystemStats(Iterable<Counter> counters, MetricsCollector collector, MetricsScope scope,
                                          Table<MetricsScope, String, Integer> previousStats) {

    // we don't want to overcount the untagged version of the metric.  For example.  If "metric":"store.bytes"
    // comes in with "tag":"dataset1" and value 10, we will also have another counter for just the metric without the
    // tag, also with value 10.  If we increment both of them with their values, the final count sent off to the metrics
    // system for the untagged metric will be 20 instead of 10.  So we need to keep track of the sum of the tagged
    // values so that we can adjust accordingly.
    Map<String, Integer> metricTagValues = Maps.newHashMap();
    Map<String, Integer> metricUntaggedValues = Maps.newHashMap();

    for (Counter counter : counters) {

      // mapred counters are running counters whereas our metrics timeseries and aggregates make more
      // sense as incremental numbers.  So we want to subtract the current counter value from the previous before
      // emitting to the metrics system.
      int emitValue = calcDiffAndSetTableValue(previousStats, scope, counter.getName(), counter.getValue());

      // "<metric>" or "<metric>,<tag>" if tag is present
      String[] parts = counter.getName().split(",", 2);
      String metric = parts[0];
      metricUntaggedValues.put(metric, emitValue);
    }

    // emit adjusted counts for the untagged metrics.
    for (Map.Entry<String, Integer> untaggedEntry : metricUntaggedValues.entrySet()) {
      String metric = untaggedEntry.getKey();
      int tagValueSum = (metricTagValues.containsKey(metric)) ? metricTagValues.get(metric) : 0;
      int adjustedValue = untaggedEntry.getValue() - tagValueSum;
      collector.increment(metric, adjustedValue);
    }
  }

  private void reportGaugeSystemStats(Iterable<Counter> counters, MetricsCollector collector) {
    for (Counter counter : counters) {
      // "<metric>" or "<metric>,<tag>" if tag is present
      String[] parts = counter.getName().split(",", 2);
      String metric = parts[0];
      collector.gauge(metric, counter.getValue());
    }
  }

  private long getTaskCounter(TaskCounter taskCounter) throws IOException, InterruptedException {
    return jobConf.getCounters().findCounter(TaskCounter.class.getName(), taskCounter.name()).getValue();
  }

}
