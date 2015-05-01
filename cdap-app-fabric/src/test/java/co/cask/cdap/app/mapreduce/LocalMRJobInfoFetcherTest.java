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

import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.MetricValues;
import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.MRJobInfo;
import co.cask.cdap.proto.MRTaskInfo;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.internal.guice.AppFabricTestModule;
import co.cask.tephra.TransactionManager;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class LocalMRJobInfoFetcherTest {
  private static Injector injector;
  private static MetricStore metricStore;

  @BeforeClass
  public static void beforeClass() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.AppFabric.OUTPUT_DIR, System.getProperty("java.io.tmpdir"));
    conf.set(Constants.AppFabric.TEMP_DIR, System.getProperty("java.io.tmpdir"));
    injector = startMetricsService(conf);
    metricStore = injector.getInstance(MetricStore.class);
  }

  public static Injector startMetricsService(CConfiguration conf) {
    Injector injector = Guice.createInjector(new AppFabricTestModule(conf));
    injector.getInstance(TransactionManager.class).startAndWait();
    injector.getInstance(DatasetOpExecutor.class).startAndWait();
    injector.getInstance(DatasetService.class).startAndWait();
    return injector;
  }

  @Test
  public void testGetMRJobInfo() throws Exception {
    Id.Program programId = Id.Program.from("fooNamespace", "testApp", ProgramType.MAPREDUCE, "fooMapReduce");
    Id.Run runId = new Id.Run(programId, "run10878");

    Map<String, String> runContext = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, programId.getNamespaceId(),
                                                     Constants.Metrics.Tag.APP, programId.getApplicationId(),
                                                     Constants.Metrics.Tag.MAPREDUCE, programId.getId(),
                                                     Constants.Metrics.Tag.RUN_ID, runId.getId());

    Map<String, String> mapTypeContext =
      addToContext(runContext, Constants.Metrics.Tag.MR_TASK_TYPE, MapReduceMetrics.TaskType.Mapper.getId());

    Map<String, String> reduceTypeContext =
      addToContext(runContext, Constants.Metrics.Tag.MR_TASK_TYPE, MapReduceMetrics.TaskType.Reducer.getId());

    String mapTask1Name = "task_m_01";
    Map<String, String> mapTask1Context = addToContext(mapTypeContext, Constants.Metrics.Tag.INSTANCE_ID, mapTask1Name);

    String mapTask2Name = "task_m_02";
    Map<String, String> mapTask2Context = addToContext(mapTypeContext, Constants.Metrics.Tag.INSTANCE_ID, mapTask2Name);

    String reduceTaskName = "task_r_01";
    Map<String, String> reduceTaskContext =
      addToContext(reduceTypeContext, Constants.Metrics.Tag.INSTANCE_ID, reduceTaskName);


    // Imitate a MapReduce Job running (gauge mapper and reducer metrics)
    long measureTime = System.currentTimeMillis() / 1000;
    gauge(mapTypeContext, MapReduceMetrics.METRIC_COMPLETION, measureTime, 76L);
    gauge(reduceTypeContext, MapReduceMetrics.METRIC_COMPLETION, measureTime, 52L);

    gauge(mapTask1Context, MapReduceMetrics.METRIC_TASK_COMPLETION, measureTime, 100L);
    gauge(mapTask1Context, MapReduceMetrics.METRIC_TASK_INPUT_RECORDS, measureTime, 32L);
    gauge(mapTask1Context, MapReduceMetrics.METRIC_TASK_OUTPUT_RECORDS, measureTime, 320L);

    gauge(mapTask2Context, MapReduceMetrics.METRIC_TASK_COMPLETION, measureTime, 12L);
    gauge(mapTask2Context, MapReduceMetrics.METRIC_TASK_INPUT_RECORDS, measureTime, 6L);
    gauge(mapTask2Context, MapReduceMetrics.METRIC_TASK_OUTPUT_RECORDS, measureTime, 60L);

    // gauge job-level counters for mappers
    gauge(mapTypeContext, MapReduceMetrics.METRIC_INPUT_RECORDS, measureTime, 38L);
    gauge(mapTypeContext, MapReduceMetrics.METRIC_OUTPUT_RECORDS, measureTime, 380L);

    gauge(reduceTaskContext, MapReduceMetrics.METRIC_TASK_COMPLETION, measureTime, 76L);
    gauge(reduceTaskContext, MapReduceMetrics.METRIC_TASK_INPUT_RECORDS, measureTime, 320L);
    gauge(reduceTaskContext, MapReduceMetrics.METRIC_TASK_OUTPUT_RECORDS, measureTime, 1L);

    // gauge job-level counters for reducers
    gauge(reduceTypeContext, MapReduceMetrics.METRIC_INPUT_RECORDS, measureTime, 320L);
    gauge(reduceTypeContext, MapReduceMetrics.METRIC_OUTPUT_RECORDS, measureTime, 1L);



    LocalMRJobInfoFetcher localMRJobInfoFetcher = injector.getInstance(LocalMRJobInfoFetcher.class);
    MRJobInfo mrJobInfo = localMRJobInfoFetcher.getMRJobInfo(runId);


    // Incomplete because MapReduceMetricsInfo does not provide task-level state and start/end times.
    Assert.assertFalse(mrJobInfo.isComplete());

    // Check job-level counters
    Map<String, Long> jobCounters = mrJobInfo.getCounters();
    Assert.assertEquals((Long) 38L, jobCounters.get(TaskCounter.MAP_INPUT_RECORDS.name()));
    Assert.assertEquals((Long) 380L, jobCounters.get(TaskCounter.MAP_OUTPUT_RECORDS.name()));
    Assert.assertEquals((Long) 320L, jobCounters.get(TaskCounter.REDUCE_INPUT_RECORDS.name()));
    Assert.assertEquals((Long) 1L, jobCounters.get(TaskCounter.REDUCE_OUTPUT_RECORDS.name()));


    // Ensure all tasks show up
    List<MRTaskInfo> mapTasks = mrJobInfo.getMapTasks();
    List<MRTaskInfo> reduceTasks = mrJobInfo.getReduceTasks();
    Assert.assertEquals(2, mapTasks.size());
    Assert.assertEquals(1, reduceTasks.size());

    MRTaskInfo mapTask1 = findByTaskId(mapTasks, mapTask1Name);
    MRTaskInfo mapTask2 = findByTaskId(mapTasks, mapTask2Name);
    MRTaskInfo reduceTask = findByTaskId(reduceTasks, reduceTaskName);

    // Check task-level counters
    Map<String, Long> mapTask1Counters = mapTask1.getCounters();
    Assert.assertEquals((Long) 32L, mapTask1Counters.get(TaskCounter.MAP_INPUT_RECORDS.name()));
    Assert.assertEquals((Long) 320L, mapTask1Counters.get(TaskCounter.MAP_OUTPUT_RECORDS.name()));

    Map<String, Long> mapTask2Counters = mapTask2.getCounters();
    Assert.assertEquals((Long) 6L, mapTask2Counters.get(TaskCounter.MAP_INPUT_RECORDS.name()));
    Assert.assertEquals((Long) 60L, mapTask2Counters.get(TaskCounter.MAP_OUTPUT_RECORDS.name()));

    Map<String, Long> reduceTaskCounters = reduceTask.getCounters();
    Assert.assertEquals((Long) 320L, reduceTaskCounters.get(TaskCounter.REDUCE_INPUT_RECORDS.name()));
    Assert.assertEquals((Long) 1L, reduceTaskCounters.get(TaskCounter.REDUCE_OUTPUT_RECORDS.name()));

    // Checking progress
    float permittedProgressDelta = 0.01F;
    Assert.assertEquals(0.76F, mrJobInfo.getMapProgress(), permittedProgressDelta);
    Assert.assertEquals(0.52F, mrJobInfo.getReduceProgress(), permittedProgressDelta);

    Assert.assertEquals(1.0F, mapTask1.getProgress(), permittedProgressDelta);
    Assert.assertEquals(0.12F, mapTask2.getProgress(), permittedProgressDelta);
    Assert.assertEquals(0.76F, reduceTask.getProgress(), permittedProgressDelta);

  }

  private void gauge(Map<String, String> context, String metric, long timestamp, Long value) throws Exception {
    metricStore.add(new MetricValues(context, metric, timestamp, value, MetricType.GAUGE));
  }

  // Returned copied map, with new key-value pair.
  private Map<String, String> addToContext(Map<String, String> context, String key, String value) {
    return ImmutableMap.<String, String>builder()
      .putAll(context)
      .put(key, value)
      .build();
  }

  private MRTaskInfo findByTaskId(List<MRTaskInfo> taskInfos, String taskId) {
    for (MRTaskInfo taskInfo : taskInfos) {
      if (taskInfo.getTaskId().equals(taskId)) {
        return taskInfo;
      }
    }
    throw new IllegalArgumentException(
      String.format("TaskId: %s not found in list of TaskInfos: %s", taskId, taskInfos));
  }
}
