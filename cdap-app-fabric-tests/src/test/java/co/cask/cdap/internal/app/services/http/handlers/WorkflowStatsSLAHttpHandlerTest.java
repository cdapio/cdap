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

package co.cask.cdap.internal.app.services.http.handlers;

import co.cask.cdap.WorkflowApp;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.MetricValues;
import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.id.Id;
import co.cask.cdap.gateway.handlers.WorkflowStatsSLAHttpHandler;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.proto.PercentileInformation;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.WorkflowStatistics;
import co.cask.cdap.proto.WorkflowStatsComparison;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.WorkflowId;
import com.google.common.collect.ImmutableMap;
import com.google.gson.reflect.TypeToken;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.http.HttpResponse;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link WorkflowStatsSLAHttpHandler}
 */
public class WorkflowStatsSLAHttpHandlerTest extends AppFabricTestBase {

  private static final ApplicationId WORKFLOW_APP = NamespaceId.DEFAULT.app("WorkflowApp");
  private static MetricStore metricStore;
  private static Store store;

  private int sourceId;

  @BeforeClass
  public static void beforeClass() throws Throwable {
    AppFabricTestBase.beforeClass();
    store = getInjector().getInstance(DefaultStore.class);
    metricStore = getInjector().getInstance(MetricStore.class);
  }

  private void setStartAndRunning(final ProgramId id, final String pid, final long startTime) {
    setStartAndRunning(id, pid, startTime, ImmutableMap.<String, String>of(),
                       ImmutableMap.<String, String>of());

  }


  private void setStartAndRunning(final ProgramId id, final String pid, final long startTime,
                                  final Map<String, String> runtimeArgs,
                                  final Map<String, String> systemArgs) {
    store.setStart(id, pid, startTime, null, runtimeArgs, systemArgs,
                   AppFabricTestHelper.createSourceId(++sourceId));
    store.setRunning(id, pid, startTime + 1, null, AppFabricTestHelper.createSourceId(++sourceId));
  }

  @Test
  public void testStatistics() throws Exception {
    deploy(WorkflowApp.class);
    String workflowName = "FunWorkflow";
    String mapreduceName = "ClassicWordCount";
    String sparkName = "SparkWorkflowTest";

    ProgramId workflowProgram = WORKFLOW_APP.workflow(workflowName);
    ProgramId mapreduceProgram = WORKFLOW_APP.mr(mapreduceName);
    ProgramId sparkProgram = WORKFLOW_APP.spark(sparkName);

    // Time from program starting to program running
    int startDelaySecs = 1;

    long startTime = System.currentTimeMillis();
    long currentTimeMillis = startTime;
    String outlierRunId = null;
    for (int i = 0; i < 10; i++) {
      // workflow runs every 5 minutes
      currentTimeMillis = startTime + (i * TimeUnit.MINUTES.toMillis(5));
      RunId workflowRunId = RunIds.generate(currentTimeMillis);
      long workflowStartTimeSecs = RunIds.getTime(workflowRunId, TimeUnit.SECONDS);
      setStartAndRunning(workflowProgram, workflowRunId.getId(),
                           workflowStartTimeSecs);

      // MR job starts 2 seconds after workflow started
      RunId mapreduceRunid = RunIds.generate(currentTimeMillis + TimeUnit.SECONDS.toMillis(2));
      Map<String, String> systemArgs = ImmutableMap.of(ProgramOptionConstants.WORKFLOW_NODE_ID, mapreduceName,
                                                       ProgramOptionConstants.WORKFLOW_NAME, workflowName,
                                                       ProgramOptionConstants.WORKFLOW_RUN_ID, workflowRunId.getId());

      workflowStartTimeSecs = RunIds.getTime(mapreduceRunid, TimeUnit.SECONDS);
      setStartAndRunning(mapreduceProgram, mapreduceRunid.getId(), workflowStartTimeSecs,
                         ImmutableMap.<String, String>of(), systemArgs);

      store.setStop(mapreduceProgram, mapreduceRunid.getId(),
                    // map-reduce job ran for 17 seconds
                    TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis) + 19,
                    ProgramRunStatus.COMPLETED, AppFabricTestHelper.createSourceId(++sourceId));

      // This makes sure that not all runs have Spark programs in them
      if (i < 5) {
        // spark starts 20 seconds after workflow starts
        RunId sparkRunid = RunIds.generate(currentTimeMillis + TimeUnit.SECONDS.toMillis(20));
        systemArgs = ImmutableMap.of(ProgramOptionConstants.WORKFLOW_NODE_ID, sparkProgram.getProgram(),
                                     ProgramOptionConstants.WORKFLOW_NAME, workflowName,
                                     ProgramOptionConstants.WORKFLOW_RUN_ID, workflowRunId.getId());
        workflowStartTimeSecs = RunIds.getTime(sparkRunid, TimeUnit.SECONDS);
        setStartAndRunning(sparkProgram, sparkRunid.getId(), workflowStartTimeSecs,
                           ImmutableMap.<String, String>of(), systemArgs);

        // spark job runs for 38 seconds
        long stopTime = TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis) + 58;
        if (i == 4) {
          // spark job ran for 100 seconds. 62 seconds greater than avg.
          stopTime = TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis) + 120;
        }
        store.setStop(sparkProgram, sparkRunid.getId(), stopTime, ProgramRunStatus.COMPLETED,
                      AppFabricTestHelper.createSourceId(++sourceId));
      }

      // workflow ran for 1 minute
      long workflowStopTime = TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis) + 60;
      if (i == 4) {
        // spark job ran longer for this run
        workflowStopTime = TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis) + 122;
        outlierRunId = workflowRunId.getId();
      }

      store.setStop(workflowProgram, workflowRunId.getId(), workflowStopTime, ProgramRunStatus.COMPLETED,
                    AppFabricTestHelper.createSourceId(++sourceId));
    }

    String request = String.format("%s/namespaces/%s/apps/%s/workflows/%s/statistics?start=%s&end=%s" +
                                     "&percentile=%s",
                                   Constants.Gateway.API_VERSION_3, Id.Namespace.DEFAULT.getId(),
                                   WorkflowApp.class.getSimpleName(), workflowProgram.getProgram(),
                                   TimeUnit.MILLISECONDS.toSeconds(startTime),
                                   TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis) + TimeUnit.MINUTES.toSeconds(2),
                                   "99");

    HttpResponse response = doGet(request);
    WorkflowStatistics workflowStatistics =
      readResponse(response, new TypeToken<WorkflowStatistics>() { }.getType());
    PercentileInformation percentileInformation = workflowStatistics.getPercentileInformationList().get(0);
    Assert.assertEquals(1, percentileInformation.getRunIdsOverPercentile().size());
    Assert.assertEquals(outlierRunId, percentileInformation.getRunIdsOverPercentile().get(0));
    Assert.assertEquals("5", workflowStatistics.getNodes().get(sparkName).get("runs"));

    request = String.format("%s/namespaces/%s/apps/%s/workflows/%s/statistics?start=%s&end=%s" +
                              "&percentile=%s&percentile=%s",
                            Constants.Gateway.API_VERSION_3, Id.Namespace.DEFAULT.getId(),
                            WorkflowApp.class.getSimpleName(), workflowProgram.getProgram(), "now", "0", "90", "95");

    response = doGet(request);
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.code(),
                        response.getStatusLine().getStatusCode());

    request = String.format("%s/namespaces/%s/apps/%s/workflows/%s/statistics?start=%s&end=%s" +
                              "&percentile=%s&percentile=%s",
                            Constants.Gateway.API_VERSION_3, Id.Namespace.DEFAULT.getId(),
                            WorkflowApp.class.getSimpleName(), workflowProgram.getProgram(), "now", "0", "90.0", "950");

    response = doGet(request);
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.code(),
                        response.getStatusLine().getStatusCode());
    Id.Application appId = new Id.Application(Id.Namespace.DEFAULT, WorkflowApp.class.getSimpleName());
    deleteApp(appId, HttpResponseStatus.OK.code());

    request = String.format("%s/namespaces/%s/apps/%s/workflows/%s/statistics?start=%s&end=%s" +
                              "&percentile=%s",
                            Constants.Gateway.API_VERSION_3, Id.Namespace.DEFAULT.getId(),
                            WorkflowApp.class.getSimpleName(), workflowProgram,
                            0,
                            System.currentTimeMillis(),
                            "99");
    response = doGet(request);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatusLine().getStatusCode());
    Assert.assertTrue(readResponse(response).startsWith("There are no statistics associated with this workflow : "));
  }


  @Test
  public void testDetails() throws Exception {
    deploy(WorkflowApp.class);
    String workflowName = "FunWorkflow";
    String mapreduceName = "ClassicWordCount";
    String sparkName = "SparkWorkflowTest";

    WorkflowId workflowProgram = WORKFLOW_APP.workflow(workflowName);
    ProgramId mapreduceProgram = WORKFLOW_APP.mr(mapreduceName);
    ProgramId sparkProgram = WORKFLOW_APP.spark(sparkName);

    List<RunId> runIdList = setupRuns(workflowProgram, mapreduceProgram, sparkProgram, store, 13);

    String request = String.format("%s/namespaces/%s/apps/%s/workflows/%s/runs/%s/statistics?limit=%s&interval=%s",
                                   Constants.Gateway.API_VERSION_3, Id.Namespace.DEFAULT.getId(),
                                   WorkflowApp.class.getSimpleName(), workflowProgram.getProgram(),
                                   runIdList.get(6).getId(), "3", "10m");

    HttpResponse response = doGet(request);
    WorkflowStatsComparison workflowStatistics =
      readResponse(response, new TypeToken<WorkflowStatsComparison>() { }.getType());

    Assert.assertEquals(7, workflowStatistics.getProgramNodesList().iterator().next()
      .getWorkflowProgramDetailsList().size());

    request = String.format("%s/namespaces/%s/apps/%s/workflows/%s/runs/%s/statistics?limit=0",
                            Constants.Gateway.API_VERSION_3, Id.Namespace.DEFAULT.getId(),
                            WorkflowApp.class.getSimpleName(), workflowProgram.getProgram(), runIdList.get(6).getId());

    response = doGet(request);
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.getStatusLine().getStatusCode());

    request = String.format("%s/namespaces/%s/apps/%s/workflows/%s/runs/%s/statistics?limit=10&interval=10",
                            Constants.Gateway.API_VERSION_3, Id.Namespace.DEFAULT.getId(),
                            WorkflowApp.class.getSimpleName(), workflowProgram.getProgram(), runIdList.get(6).getId());

    response = doGet(request);
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.getStatusLine().getStatusCode());

    request = String.format("%s/namespaces/%s/apps/%s/workflows/%s/runs/%s/statistics?limit=10&interval=10P",
                            Constants.Gateway.API_VERSION_3, Id.Namespace.DEFAULT.getId(),
                            WorkflowApp.class.getSimpleName(), workflowProgram.getProgram(), runIdList.get(6).getId());

    response = doGet(request);
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.getStatusLine().getStatusCode());

    request = String.format("%s/namespaces/%s/apps/%s/workflows/%s/runs/%s/statistics?limit=20&interval=0d",
                            Constants.Gateway.API_VERSION_3, Id.Namespace.DEFAULT.getId(),
                            WorkflowApp.class.getSimpleName(), workflowProgram.getProgram(), runIdList.get(6).getId());

    response = doGet(request);
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.getStatusLine().getStatusCode());
  }

  @Test
  public void testCompare() throws Exception {
    deploy(WorkflowApp.class);
    String workflowName = "FunWorkflow";
    String mapreduceName = "ClassicWordCount";
    String sparkName = "SparkWorkflowTest";

    WorkflowId workflowProgram = WORKFLOW_APP.workflow(workflowName);
    ProgramId mapreduceProgram = WORKFLOW_APP.mr(mapreduceName);
    ProgramId sparkProgram = WORKFLOW_APP.spark(sparkName);

    List<RunId> workflowRunIdList = setupRuns(workflowProgram, mapreduceProgram, sparkProgram, store, 2);
    RunId workflowRun1 = workflowRunIdList.get(0);
    RunId workflowRun2 = workflowRunIdList.get(1);

    String request = String.format("%s/namespaces/%s/apps/%s/workflows/%s/runs/%s/compare?other-run-id=%s",
                                   Constants.Gateway.API_VERSION_3, Id.Namespace.DEFAULT.getId(),
                                   WorkflowApp.class.getSimpleName(), workflowProgram.getProgram(),
                                   workflowRun1.getId(), workflowRun2.getId());

    HttpResponse response = doGet(request);
    Collection<WorkflowStatsComparison.ProgramNodes> workflowStatistics =
      readResponse(response, new TypeToken<Collection<WorkflowStatsComparison.ProgramNodes>>() {
      }.getType());

    Assert.assertNotNull(workflowStatistics.iterator().next());
    Assert.assertEquals(2, workflowStatistics.size());

    for (WorkflowStatsComparison.ProgramNodes node : workflowStatistics) {
      if (node.getProgramType() == ProgramType.MAPREDUCE) {
        Assert.assertEquals(38L, (long) node.getWorkflowProgramDetailsList().get(0)
          .getMetrics().get(TaskCounter.MAP_INPUT_RECORDS.name()));
      }
    }
  }

  /*
   * This helper is used only for the details and compare endpoints and not the statistics endpoint because
   * the statistics endpoint needs to handle number of spark runs differently and also have tests for a
   * specific run's spark job.
   */
  private List<RunId> setupRuns(WorkflowId workflowProgram, ProgramId mapreduceProgram,
                                ProgramId sparkProgram, Store store, int count) throws Exception {
    List<RunId> runIdList = new ArrayList<>();
    long startTime = System.currentTimeMillis();
    long currentTimeMillis;

    // Time in seconds from program starting to program running
    int startDelaySecs = 5;

    for (int i = 0; i < count; i++) {
      // work-flow runs every 5 minutes
      currentTimeMillis = startTime + (i * TimeUnit.MINUTES.toMillis(5));
      RunId workflowRunId = RunIds.generate(currentTimeMillis);
      runIdList.add(workflowRunId);
      long workflowStartTimeSecs = TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis);
      setStartAndRunning(workflowProgram, workflowRunId.getId(), workflowStartTimeSecs
      );

      // MR job starts 2 seconds after workflow started
      RunId mapreduceRunid = RunIds.generate(currentTimeMillis + TimeUnit.SECONDS.toMillis(2));
      Map<String, String> systemArgs = ImmutableMap.of(ProgramOptionConstants.WORKFLOW_NODE_ID,
                                                       mapreduceProgram.getProgram(),
                                                       ProgramOptionConstants.WORKFLOW_NAME,
                                                       workflowProgram.getProgram(),
                                                       ProgramOptionConstants.WORKFLOW_RUN_ID, workflowRunId.getId());

      workflowStartTimeSecs = RunIds.getTime(mapreduceRunid, TimeUnit.SECONDS);
      setStartAndRunning(mapreduceProgram, mapreduceRunid.getId(), workflowStartTimeSecs,
                         ImmutableMap.<String, String>of(), systemArgs);
      store.setStop(mapreduceProgram, mapreduceRunid.getId(),
                    // map-reduce job ran for 17 seconds
                    TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis) + 19,
                    ProgramRunStatus.COMPLETED, AppFabricTestHelper.createSourceId(++sourceId));

      Map<String, String> mapTypeContext = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE,
                                                           mapreduceProgram.getNamespace(),
                                                           Constants.Metrics.Tag.APP,
                                                           mapreduceProgram.getApplication(),
                                                           Constants.Metrics.Tag.MAPREDUCE,
                                                           mapreduceProgram.getProgram(),
                                                           Constants.Metrics.Tag.RUN_ID, mapreduceRunid.toString(),
                                                           Constants.Metrics.Tag.MR_TASK_TYPE,
                                                           MapReduceMetrics.TaskType.Mapper.getId());

      metricStore.add(new MetricValues(mapTypeContext, MapReduceMetrics.METRIC_INPUT_RECORDS, 10, 38L,
                                       MetricType.GAUGE));

      // spark starts 20 seconds after workflow starts
      systemArgs = ImmutableMap.of(ProgramOptionConstants.WORKFLOW_NODE_ID, sparkProgram.getProgram(),
                                   ProgramOptionConstants.WORKFLOW_NAME, workflowProgram.getProgram(),
                                   ProgramOptionConstants.WORKFLOW_RUN_ID, workflowRunId.getId());
      RunId sparkRunid = RunIds.generate(currentTimeMillis + TimeUnit.SECONDS.toMillis(20));
      workflowStartTimeSecs = RunIds.getTime(sparkRunid, TimeUnit.SECONDS);
      setStartAndRunning(sparkProgram, sparkRunid.getId(), workflowStartTimeSecs,
                         ImmutableMap.<String, String>of(), systemArgs);

      // spark job runs for 38 seconds
      long stopTime = TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis) + 58;
      store.setStop(sparkProgram, sparkRunid.getId(), stopTime, ProgramRunStatus.COMPLETED,
                    AppFabricTestHelper.createSourceId(++sourceId));

      // workflow ran for 1 minute
      long workflowStopTime = TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis) + 60;

      store.setStop(workflowProgram, workflowRunId.getId(), workflowStopTime, ProgramRunStatus.COMPLETED,
                    AppFabricTestHelper.createSourceId(++sourceId));
    }
    return runIdList;
  }
}
