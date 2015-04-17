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

package co.cask.cdap.app.mapreduce;

import co.cask.cdap.api.dataset.lib.cube.TimeValue;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.MRJobInfo;
import co.cask.cdap.proto.MRTaskInfo;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.inject.Inject;
import org.apache.hadoop.mapreduce.TaskCounter;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Retrieves information/reports for a MapReduce run via the Metrics system.
 */
public class MapReduceMetricsInfo {

  private final MetricStore metricStore;

  @Inject
  public MapReduceMetricsInfo(MetricStore metricStore) {
    this.metricStore = metricStore;
  }

  /**
   * @param runId for which information will be returned.
   * @return a {@link MRJobInfo} containing information about a particular MapReduce program run.
   */
  public MRJobInfo getMRJobInfo(Id.Run runId) throws Exception {
    Preconditions.checkArgument(ProgramType.MAPREDUCE.equals(runId.getProgram().getType()));

    // baseTags has tag keys: ns.app.mr.runid
    Map<String, String> baseTags = Maps.newHashMap();
    baseTags.put(Constants.Metrics.Tag.NAMESPACE, runId.getNamespace().getId());
    baseTags.put(Constants.Metrics.Tag.APP, runId.getProgram().getApplicationId());
    baseTags.put(Constants.Metrics.Tag.MAPREDUCE, runId.getProgram().getId());
    baseTags.put(Constants.Metrics.Tag.RUN_ID, runId.getId());

    Map<String, String> mapTags = Maps.newHashMap(baseTags);
    mapTags.put(Constants.Metrics.Tag.MR_TASK_TYPE, MapReduceMetrics.TaskType.Mapper.getId());

    Map<String, String> reduceTags = Maps.newHashMap(baseTags);
    reduceTags.put(Constants.Metrics.Tag.MR_TASK_TYPE, MapReduceMetrics.TaskType.Reducer.getId());

    // map from RunId -> (CounterName -> CounterValue)
    Table<String, String, Long> mapTaskMetrics = HashBasedTable.create();
    Table<String, String, Long> reduceTaskMetrics = HashBasedTable.create();


    // Populate mapTaskMetrics and reduce Task Metrics via MetricStore. Used to construct MRTaskInfo below.
    // Use batch-querying when it is available on the MetricStore. https://issues.cask.co/browse/CDAP-2045
    putMetrics(queryGroupedAggregates(mapTags, MapReduceMetrics.METRIC_INPUT_RECORDS),
               mapTaskMetrics, TaskCounter.MAP_INPUT_RECORDS.name());
    putMetrics(queryGroupedAggregates(mapTags, MapReduceMetrics.METRIC_OUTPUT_RECORDS),
               mapTaskMetrics, TaskCounter.MAP_OUTPUT_RECORDS.name());
    putMetrics(queryGroupedAggregates(mapTags, MapReduceMetrics.METRIC_BYTES),
               mapTaskMetrics, TaskCounter.MAP_OUTPUT_BYTES.name());

    Map<String, Long> mapProgress = queryGroupedAggregates(mapTags, MapReduceMetrics.METRIC_TASK_COMPLETION);

    putMetrics(queryGroupedAggregates(reduceTags, MapReduceMetrics.METRIC_INPUT_RECORDS),
               reduceTaskMetrics, TaskCounter.REDUCE_INPUT_RECORDS.name());
    putMetrics(queryGroupedAggregates(reduceTags, MapReduceMetrics.METRIC_OUTPUT_RECORDS),
               reduceTaskMetrics, TaskCounter.REDUCE_OUTPUT_RECORDS.name());

    Map<String, Long> reduceProgress = queryGroupedAggregates(reduceTags, MapReduceMetrics.METRIC_TASK_COMPLETION);

    // Construct MRTaskInfos from the information we can get from Metric system.
    List<MRTaskInfo> mapTaskInfos = Lists.newArrayList();
    for (Map.Entry<String, Map<String, Long>> taskEntry : mapTaskMetrics.rowMap().entrySet()) {
      String mapTaskId = taskEntry.getKey();
      mapTaskInfos.add(new MRTaskInfo(mapTaskId, null, null, null,
                                      mapProgress.get(mapTaskId) / 100.0F, taskEntry.getValue()));
    }

    List<MRTaskInfo> reduceTaskInfos = Lists.newArrayList();
    for (Map.Entry<String, Map<String, Long>> taskEntry : reduceTaskMetrics.rowMap().entrySet()) {
      String reduceTaskId = taskEntry.getKey();
      reduceTaskInfos.add(new MRTaskInfo(reduceTaskId, null, null, null,
                                         reduceProgress.get(reduceTaskId) / 100.0F, taskEntry.getValue()));
    }

    return getJobCounters(mapTags, reduceTags, mapTaskInfos, reduceTaskInfos);
  }


  /**
   * Copies metric values from {@param taskMetrics} to {@param allTaskMetrics}.
   * This is necessary because we read metrics from MetricStore in batch (one query for all map or reduce tasks
   * via the MetricStore GroupBy functionality).
   * @param taskMetrics mapping: TaskId -> metricValue
   * @param allTaskMetrics mapping: TaskId -> (CounterName -> CounterValue)
   * @param counterName the name of the key to copy over
   */
  private void putMetrics(Map<String, Long> taskMetrics, Table<String, String, Long> allTaskMetrics,
                          String counterName) {
    for (Map.Entry<String, Long> entry : taskMetrics.entrySet()) {
      allTaskMetrics.put(entry.getKey(), counterName, entry.getValue());
    }
  }

  private MRJobInfo getJobCounters(Map<String, String> mapTags, Map<String, String> reduceTags,
                                   List<MRTaskInfo> mapTaskInfos, List<MRTaskInfo> reduceTaskInfos) throws Exception {
    HashMap<String, Long> metrics = Maps.newHashMap();
    // Use batch-querying when it is available on the MetricStore. https://issues.cask.co/browse/CDAP-2045
    metrics.put(TaskCounter.MAP_INPUT_RECORDS.name(),
                getAggregates(mapTags, MapReduceMetrics.METRIC_INPUT_RECORDS));
    metrics.put(TaskCounter.MAP_OUTPUT_RECORDS.name(),
                getAggregates(mapTags, MapReduceMetrics.METRIC_OUTPUT_RECORDS));
    metrics.put(TaskCounter.MAP_OUTPUT_BYTES.name(),
                getAggregates(mapTags, MapReduceMetrics.METRIC_BYTES));

    metrics.put(TaskCounter.REDUCE_INPUT_RECORDS.name(),
                getAggregates(reduceTags, MapReduceMetrics.METRIC_INPUT_RECORDS));
    metrics.put(TaskCounter.REDUCE_OUTPUT_RECORDS.name(),
                getAggregates(reduceTags, MapReduceMetrics.METRIC_OUTPUT_RECORDS));

    float mapProgress = getAggregates(mapTags, MapReduceMetrics.METRIC_COMPLETION) / 100.0F;
    float reduceProgress = getAggregates(reduceTags, MapReduceMetrics.METRIC_COMPLETION) / 100.0F;

    return new MRJobInfo(null, null, null, mapProgress, reduceProgress, metrics, mapTaskInfos, reduceTaskInfos);
  }

  private String prependSystem(String metric) {
    return "system." + metric;
  }

  private long getAggregates(Map<String, String> tags, String metric) throws Exception {
    MetricDataQuery metricDataQuery =
      new MetricDataQuery(0, Integer.MAX_VALUE, Integer.MAX_VALUE, ImmutableList.of(prependSystem(metric)),
                          MetricType.COUNTER, tags, ImmutableList.<String>of());
    Collection<MetricTimeSeries> query = metricStore.query(metricDataQuery);
    if (query.isEmpty()) {
      return 0;
    }
    MetricTimeSeries timeSeries = Iterables.getOnlyElement(query);
    List<TimeValue> timeValues = timeSeries.getTimeValues();
    TimeValue timeValue = Iterables.getOnlyElement(timeValues);
    return timeValue.getValue();
  }

  // queries MetricStore for one metric across all tasks of a certain TaskType, using GroupBy InstanceId
  private Map<String, Long> queryGroupedAggregates(Map<String, String> tags, String metric) throws Exception {
    MetricDataQuery metricDataQuery =
      new MetricDataQuery(0, Integer.MAX_VALUE, Integer.MAX_VALUE, ImmutableList.of(prependSystem(metric)),
                          MetricType.GAUGE, tags, ImmutableList.of(Constants.Metrics.Tag.INSTANCE_ID));
    Collection<MetricTimeSeries> query = metricStore.query(metricDataQuery);

    // runId -> metricValue
    Map<String, Long> taskMetrics = Maps.newHashMap();
    for (MetricTimeSeries metricTimeSeries : query) {
      List<TimeValue> timeValues = metricTimeSeries.getTimeValues();
      TimeValue timeValue = Iterables.getOnlyElement(timeValues);
      String taskId = metricTimeSeries.getTagValues().get(Constants.Metrics.Tag.INSTANCE_ID);
      taskMetrics.put(taskId, timeValue.getValue());
    }
    return taskMetrics;
  }
}
