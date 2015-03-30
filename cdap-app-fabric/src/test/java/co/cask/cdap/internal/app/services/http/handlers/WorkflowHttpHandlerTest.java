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

package co.cask.cdap.internal.app.services.http.handlers;

import co.cask.cdap.AppWithSchedule;
import co.cask.cdap.AppWithStreamSizeSchedule;
import co.cask.cdap.ConcurrentWorkflowApp;
import co.cask.cdap.PauseResumeWorklowApp;
import co.cask.cdap.WorkflowAppWithErrorRuns;
import co.cask.cdap.WorkflowAppWithFork;
import co.cask.cdap.WorkflowAppWithScopedParameters;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.gateway.handlers.WorkflowHttpHandler;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.proto.codec.ScheduleSpecificationCodec;
import co.cask.cdap.proto.codec.WorkflowActionSpecificationCodec;
import co.cask.cdap.test.XSlowTests;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link WorkflowHttpHandler}
 */
public class WorkflowHttpHandlerTest  extends AppFabricTestBase {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(ScheduleSpecification.class, new ScheduleSpecificationCodec())
    .registerTypeAdapter(WorkflowActionSpecification.class, new WorkflowActionSpecificationCodec())
    .create();

  private static final String PAUSE_RESUME_WORKFLOW_APP = "PauseResumeWorkflowApp";
  private static final String PAUSE_RESUME_WORKFLOW = "PauseResumeWorkflow";
  private static final String APP_WITH_CONCURRENT_WORKFLOW = "ConcurrentWorkflowApp";
  private static final String APP_WITH_CONCURRENT_WORKFLOW_SCHEDULE_1 = "concurrentWorkflowSchedule1";
  private static final String APP_WITH_CONCURRENT_WORKFLOW_SCHEDULE_2 = "concurrentWorkflowSchedule2";
  private static final String CONCURRENT_WORKFLOW_NAME = "ConcurrentWorkflow";
  private static final String WORKFLOW_APP_WITH_FORK = "WorkflowAppWithFork";
  private static final String WORKFLOW_WITH_FORK = "WorkflowWithFork";
  private static final String WORKFLOW_APP_WITH_SCOPED_PARAMETERS = "WorkflowAppWithScopedParameters";
  private static final String WORKFLOW_APP_WITH_SCOPED_PARAMETERS_WORKFLOW = "OneWorkflow";
  private static final String APP_WITH_SCHEDULE_APP_NAME = "AppWithSchedule";
  private static final String APP_WITH_SCHEDULE_WORKFLOW_NAME = "SampleWorkflow";
  private static final String APP_WITH_SCHEDULE_SCHEDULE_NAME = "SampleSchedule";
  private static final String APP_WITH_STREAM_SCHEDULE_APP_NAME = "AppWithStreamSizeSchedule";
  private static final String APP_WITH_STREAM_SCHEDULE_SCHEDULE_NAME_1 = "SampleSchedule1";
  private static final String APP_WITH_STREAM_SCHEDULE_SCHEDULE_NAME_2 = "SampleSchedule2";
  private static final String APP_WITH_STREAM_SCHEDULE_WORKFLOW_NAME = "SampleWorkflow";
  private static final String APP_WITH_STREAM_SCHEDULE_STREAM_NAME = "stream";
  private static final String WORKFLOW_APP_WITH_ERROR_RUNS = "WorkflowAppWithErrorRuns";
  private static final String WORKFLOW_WITH_ERROR_RUNS = "WorkflowWithErrorRuns";
  private static final String WORKFLOW_WITH_ERROR_RUNS_SCHEDULE = "SampleSchedule";

  protected static final Type LIST_WORKFLOWACTIONNODE_TYPE = new TypeToken<List<WorkflowActionNode>>()
  { }.getType();

  private String getRunsUrl(String namespace, String appName, String workflow, String status) {
    String runsUrl = String.format("apps/%s/workflows/%s/runs?status=%s", appName, workflow, status);
    return getVersionedAPIPath(runsUrl, Constants.Gateway.API_VERSION_3_TOKEN, namespace);
  }

  private List<Map<String, String>> verifyAndGetWorkflowRuns(Id.Program program, String status) throws Exception {
    return verifyAndGetWorkflowRuns(program, status, 0);
  }

  private List<Map<String, String>> verifyAndGetWorkflowRuns(Id.Program program, String status, int expected)
    throws Exception {
    String path = String.format("apps/%s/workflows/%s/runs?status=%s", program.getApplicationId(), program.getId(),
                                status);
    int trial = 0, retries = 60;
    int workflowRuns = 0;
    List<Map<String, String>> history = Lists.newArrayList();
    String json;
    HttpResponse response;
    while (trial++ < retries) {
      response = doGet(getVersionedAPIPath(path, program.getNamespaceId()));
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      json = EntityUtils.toString(response.getEntity());
      history = new Gson().fromJson(json, LIST_MAP_STRING_STRING_TYPE);
      workflowRuns = history.size();
      if (workflowRuns > expected) {
        break;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertTrue(workflowRuns > expected);
    return history;
  }

  private void verifyRunningProgramCount(Id.Program program, String runId, int expected) throws Exception {
    String path = String.format("apps/%s/workflows/%s/%s/current", program.getApplicationId(), program.getId(), runId);
    int trial = 0, retries = 10;
    String json;
    List<WorkflowActionNode> output = null;
    HttpResponse response;
    while (trial++ < retries) {
      response = doGet(getVersionedAPIPath(path, program.getNamespaceId()));
      if (response.getStatusLine().getStatusCode() == 200) {
        json = EntityUtils.toString(response.getEntity());
        output = GSON.fromJson(json, LIST_WORKFLOWACTIONNODE_TYPE);
        if (output.size() == expected) {
          break;
        }
      }
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertNotNull(output);
    Assert.assertTrue(output.size() == expected);
  }

  private void suspendWorkflow(Id.Program program, String runId, int expectedStatusCode) throws Exception {
    String path = String.format("apps/%s/workflows/%s/%s/suspend", program.getApplicationId(), program.getId(), runId);
    HttpResponse response = doPost(getVersionedAPIPath(path, Constants.Gateway.API_VERSION_3_TOKEN,
                                                       program.getNamespaceId()));
    Assert.assertEquals(expectedStatusCode, response.getStatusLine().getStatusCode());
  }

  private void setAndTestRuntimeArgs(Id.Program programId, Map<String, String> args) throws Exception {
    HttpResponse response;
    String argString = GSON.toJson(args, new TypeToken<Map<String, String>>() { }.getType());
    String path = String.format("apps/%s/workflows/%s/runtimeargs", programId.getApplicationId(), programId.getId());
    String versionedRuntimeArgsUrl = getVersionedAPIPath(path, Constants.Gateway.API_VERSION_3_TOKEN,
                                                         programId.getNamespaceId());
    response = doPut(versionedRuntimeArgsUrl, argString);

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doGet(versionedRuntimeArgsUrl);

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String responseEntity = EntityUtils.toString(response.getEntity());
    Map<String, String> argsRead = GSON.fromJson(responseEntity, new TypeToken<Map<String, String>>() { }.getType());

    Assert.assertEquals(args.size(), argsRead.size());
  }


  /**
   * Tries to resume a Workflow and expect the call completed with the status.
   */
  private void resumeWorkflow(Id.Program program, String runId, int expectedStatusCode) throws Exception {
    String path = String.format("apps/%s/workflows/%s/%s/resume", program.getApplicationId(), program.getId(), runId);
    HttpResponse response = doPost(getVersionedAPIPath(path, Constants.Gateway.API_VERSION_3_TOKEN,
                                                       program.getNamespaceId()));
    Assert.assertEquals(expectedStatusCode, response.getStatusLine().getStatusCode());
  }

  private HttpResponse getWorkflowCurrentStatus(Id.Program program, String runId) throws Exception {
    String currentUrl = String.format("apps/%s/workflows/%s/%s/current", program.getApplicationId(), program.getId(),
                                      runId);
    String versionedUrl = getVersionedAPIPath(currentUrl, Constants.Gateway.API_VERSION_3_TOKEN,
                                              program.getNamespaceId());
    return doGet(versionedUrl);
  }

  private String createInput(String folderName) throws IOException {
    File inputDir = tmpFolder.newFolder(folderName);

    File inputFile = new File(inputDir.getPath() + "/words.txt");
    BufferedWriter writer = new BufferedWriter(new FileWriter(inputFile));
    try {
      writer.write("this text has");
      writer.newLine();
      writer.write("two words text inside");
    } finally {
      writer.close();
    }
    return inputDir.getAbsolutePath();
  }

  private Long getNextScheduledRunTime(Id.Program program, String schedule) throws Exception {
    String nextRunTimeUrl = String.format("apps/%s/workflows/%s/nextruntime", program.getApplicationId(),
                                          program.getId());
    String versionedUrl = getVersionedAPIPath(nextRunTimeUrl, Constants.Gateway.API_VERSION_3_TOKEN,
                                              program.getNamespaceId());
    HttpResponse response = doGet(versionedUrl);
    JsonArray array = readResponse(response, JsonArray.class);
    JsonObject wfObject = (JsonObject) array.get(0);
    Assert.assertNotNull(wfObject);
    String id = wfObject.get("id").getAsString();
    Long time = wfObject.get("time").getAsLong();
    Assert.assertTrue(id.contains(schedule));
    return time;
  }

  private String getStatusURL(String namespace, String appName, String schedule) throws Exception {
    String statusURL = String.format("apps/%s/schedules/%s/status", appName, schedule);
    return getVersionedAPIPath(statusURL, Constants.Gateway.API_VERSION_3_TOKEN, namespace);
  }

  @Test
  public void testWorkflowPauseResume() throws Exception {
    // Files used to synchronize between this test and workflow execution
    File firstSimpleActionFile = new File(tmpFolder.newFolder() + "/firstsimpleaction.file");
    File firstSimpleActionDoneFile = new File(tmpFolder.newFolder() + "/firstsimpleaction.file.done");

    File forkedSimpleActionFile = new File(tmpFolder.newFolder() + "/forkedsimpleaction.file");
    File forkedSimpleActionDoneFile = new File(tmpFolder.newFolder() + "/forkedsimpleaction.file.done");

    File anotherForkedSimpleActionFile = new File(tmpFolder.newFolder() + "/anotherforkedsimpleaction.file");
    File anotherForkedSimpleActionDoneFile = new File(tmpFolder.newFolder() + "/anotherforkedsimpleaction.file.done");

    File lastSimpleActionFile = new File(tmpFolder.newFolder() + "/lastsimpleaction.file");
    File lastSimpleActionDoneFile = new File(tmpFolder.newFolder() + "/lastsimpleaction.file.done");

    HttpResponse response = deploy(PauseResumeWorklowApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Id.Program programId = Id.Program.from(TEST_NAMESPACE2, PAUSE_RESUME_WORKFLOW_APP, ProgramType.WORKFLOW,
                                           PAUSE_RESUME_WORKFLOW);

    Map<String, String> runtimeArguments = Maps.newHashMap();
    runtimeArguments.put("first.simple.action.file", firstSimpleActionFile.getAbsolutePath());
    runtimeArguments.put("first.simple.action.donefile", firstSimpleActionDoneFile.getAbsolutePath());
    runtimeArguments.put("forked.simple.action.file", forkedSimpleActionFile.getAbsolutePath());
    runtimeArguments.put("forked.simple.action.donefile", forkedSimpleActionDoneFile.getAbsolutePath());
    runtimeArguments.put("anotherforked.simple.action.file", anotherForkedSimpleActionFile.getAbsolutePath());
    runtimeArguments.put("anotherforked.simple.action.donefile", anotherForkedSimpleActionDoneFile.getAbsolutePath());
    runtimeArguments.put("last.simple.action.file", lastSimpleActionFile.getAbsolutePath());
    runtimeArguments.put("last.simple.action.donefile", lastSimpleActionDoneFile.getAbsolutePath());

    setAndTestRuntimeArgs(programId, runtimeArguments);

    // Start the Workflow
    startProgram(programId, 200);

    // Workflow should be running
    waitState(programId, "RUNNING");

    // Get runid for the running Workflow
    List<Map<String, String>> historyRuns = verifyAndGetWorkflowRuns(programId, "running");
    Assert.assertTrue(historyRuns.size() == 1);
    String runId = historyRuns.get(0).get("runid");

    while (!firstSimpleActionFile.exists()) {
      TimeUnit.SECONDS.sleep(1);
    }

    // Only one Workflow node should be running
    verifyRunningProgramCount(programId, runId, 1);

    // Suspend the Workflow
    suspendWorkflow(programId, runId, 200);

    // Workflow status hould be SUSPENDED
    waitState(programId, "SUSPENDED");

    // Meta store information for this Workflow should reflect suspended run
    verifyAndGetWorkflowRuns(programId, "suspended");

    // Suspending the already suspended Workflow should give CONFLICT
    suspendWorkflow(programId, runId, 409);

    // Signal the FirstSimpleAction in the Workflow to continue
    firstSimpleActionDoneFile.createNewFile();

    // Even if the Workflow is suspended, currently executing action will complete and currently running nodes
    // should be zero
    verifyRunningProgramCount(programId, runId, 0);

    // Verify that Workflow is still suspended
    verifyAndGetWorkflowRuns(programId, "suspended");

    // Resume the execution of the Workflow
    resumeWorkflow(programId, runId, 200);

    // Workflow should be running
    waitState(programId, "RUNNING");

    verifyAndGetWorkflowRuns(programId, "running");

    // Resume on already running Workflow should give conflict
    resumeWorkflow(programId, runId, 409);

    // Wait till fork execution in the Workflow starts
    while (!forkedSimpleActionFile.exists() && !anotherForkedSimpleActionFile.exists()) {
      TimeUnit.SECONDS.sleep(1);
    }

    // Workflow should have 2 nodes running because of the fork
    verifyRunningProgramCount(programId, runId, 2);

    // Suspend the Workflow
    suspendWorkflow(programId, runId, 200);

    // Status of the Workflow should be suspended
    waitState(programId, "SUSPENDED");

    // Store should reflect the suspended status of the Workflow
    verifyAndGetWorkflowRuns(programId, "suspended");

    // Allow currently executing actions to complete
    forkedSimpleActionDoneFile.createNewFile();
    anotherForkedSimpleActionDoneFile.createNewFile();

    // Workflow should have zero actions running
    verifyRunningProgramCount(programId, runId, 0);

    verifyAndGetWorkflowRuns(programId, "suspended");

    Assert.assertTrue(!lastSimpleActionFile.exists());

    resumeWorkflow(programId, runId, 200);

    waitState(programId, "RUNNING");

    while (!lastSimpleActionFile.exists()) {
      TimeUnit.SECONDS.sleep(1);
    }

    verifyRunningProgramCount(programId, runId, 1);

    lastSimpleActionDoneFile.createNewFile();

    verifyAndGetWorkflowRuns(programId, "completed");

    waitState(programId, "STOPPED");
  }

  @Category(XSlowTests.class)
  @Test
  public void testMultipleWorkflowInstances() throws Exception {
    // create app in default namespace so that v2 and v3 api can be tested in the same test
    String defaultNamespace = "default";
    HttpResponse response = deploy(ConcurrentWorkflowApp.class, Constants.Gateway.API_VERSION_3_TOKEN,
                                   defaultNamespace);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Assert.assertEquals(200, resumeSchedule(defaultNamespace, APP_WITH_CONCURRENT_WORKFLOW,
                                            APP_WITH_CONCURRENT_WORKFLOW_SCHEDULE_1));
    Assert.assertEquals(200, resumeSchedule(defaultNamespace, APP_WITH_CONCURRENT_WORKFLOW,
                                            APP_WITH_CONCURRENT_WORKFLOW_SCHEDULE_2));

    Map<String, String> propMap = Maps.newHashMap();
    propMap.put(ProgramOptionConstants.CONCURRENT_RUNS_ENABLED, "true");
    PreferencesStore store = getInjector().getInstance(PreferencesStore.class);
    store.setProperties(defaultNamespace, APP_WITH_CONCURRENT_WORKFLOW, ProgramType.WORKFLOW.getCategoryName(),
                        CONCURRENT_WORKFLOW_NAME, propMap);

    Id.Program programId = Id.Program.from(defaultNamespace, APP_WITH_CONCURRENT_WORKFLOW, ProgramType.WORKFLOW,
                                           CONCURRENT_WORKFLOW_NAME);

    List<Map<String, String>> historyRuns = verifyAndGetWorkflowRuns(programId, "RUNNING", 1);

    // Suspend ConcurrentWorkflow schedules
    List<ScheduleSpecification> schedules = getSchedules(defaultNamespace, APP_WITH_CONCURRENT_WORKFLOW,
                                                         CONCURRENT_WORKFLOW_NAME);

    for (ScheduleSpecification spec : schedules) {
      Assert.assertEquals(200, suspendSchedule(defaultNamespace, APP_WITH_CONCURRENT_WORKFLOW,
                                               spec.getSchedule().getName()));
    }

    response = getWorkflowCurrentStatus(programId, historyRuns.get(0).get("runid"));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String json = EntityUtils.toString(response.getEntity());
    List<WorkflowActionNode> nodes = GSON.fromJson(json, LIST_WORKFLOWACTIONNODE_TYPE);
    Assert.assertEquals(1, nodes.size());
    Assert.assertEquals("SleepAction", nodes.get(0).getProgram().getProgramName());

    response = getWorkflowCurrentStatus(programId, historyRuns.get(1).get("runid"));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    json = EntityUtils.toString(response.getEntity());
    nodes = GSON.fromJson(json, LIST_WORKFLOWACTIONNODE_TYPE);
    Assert.assertEquals(1, nodes.size());
    Assert.assertEquals("SleepAction", nodes.get(0).getProgram().getProgramName());

    // delete the application
    String deleteURL = getVersionedAPIPath("apps/" + APP_WITH_CONCURRENT_WORKFLOW, Constants.Gateway
      .API_VERSION_3_TOKEN, defaultNamespace);
    deleteApplication(60, deleteURL, 200);
  }

  @Test
  public void testWorkflowForkApp() throws Exception {
    File doneFile = new File(tmpFolder.newFolder() + "/testWorkflowForkApp.done");
    File oneActionFile = new File(tmpFolder.newFolder() + "/oneAction.done");
    File anotherActionFile = new File(tmpFolder.newFolder() + "/anotherAction.done");

    HttpResponse response = deploy(WorkflowAppWithFork.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Id.Program programId = Id.Program.from(TEST_NAMESPACE2, WORKFLOW_APP_WITH_FORK, ProgramType.WORKFLOW,
                                           WORKFLOW_WITH_FORK);

    Map<String, String> runtimeArguments = ImmutableMap.of("done.file", doneFile.getAbsolutePath(),
                                                           "oneaction.file", oneActionFile.getAbsolutePath(),
                                                           "anotheraction.file", anotherActionFile.getAbsolutePath());

    setAndTestRuntimeArgs(programId, runtimeArguments);

    startProgram(programId, 200);

    List<Map<String, String>> historyRuns = verifyAndGetWorkflowRuns(programId, "running");
    Assert.assertTrue(historyRuns.size() == 1);

    String runId = historyRuns.get(0).get("runid");

    while (!oneActionFile.exists() && !anotherActionFile.exists()) {
      TimeUnit.SECONDS.sleep(1);
    }

    verifyRunningProgramCount(programId, runId, 2);

    stopProgram(programId, 200);

    response = getWorkflowCurrentStatus(programId, runId);
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    verifyAndGetWorkflowRuns(programId, "killed");

    oneActionFile.delete();
    anotherActionFile.delete();

    startProgram(programId, 200);

    historyRuns = verifyAndGetWorkflowRuns(programId, "running");
    Assert.assertTrue(historyRuns.size() == 1);
    runId = historyRuns.get(0).get("runid");

    while (!oneActionFile.exists() && !anotherActionFile.exists()) {
      TimeUnit.SECONDS.sleep(1);
    }

    verifyRunningProgramCount(programId, runId, 2);

    // Signal the Workflow that execution can be continued by creating temp file
    doneFile.createNewFile();

    verifyAndGetWorkflowRuns(programId, "completed");
  }

  @Category(XSlowTests.class)
  @Test
  public void testWorkflowScopedArguments() throws Exception {
    HttpResponse response = deploy(WorkflowAppWithScopedParameters.class, Constants.Gateway.API_VERSION_3_TOKEN,
                                   TEST_NAMESPACE2);

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Id.Program programId = Id.Program.from(TEST_NAMESPACE2, WORKFLOW_APP_WITH_SCOPED_PARAMETERS, ProgramType.WORKFLOW,
                                           WORKFLOW_APP_WITH_SCOPED_PARAMETERS_WORKFLOW);

    Map<String, String> runtimeArguments = Maps.newHashMap();

    runtimeArguments.put("debug", "true");
    runtimeArguments.put("mapreduce.*.debug", "false");
    runtimeArguments.put("mapreduce.OneMR.debug", "true");

    runtimeArguments.put("input.path", createInput("ProgramInput"));
    runtimeArguments.put("mapreduce.OneMR.input.path", createInput("OneMRInput"));
    runtimeArguments.put("mapreduce.AnotherMR.input.path", createInput("AnotherMRInput"));
    runtimeArguments.put("spark.*.input.path", createInput("SparkInput"));

    runtimeArguments.put("output.path", new File(tmpFolder.newFolder(), "ProgramOutput").getAbsolutePath());
    runtimeArguments.put("mapreduce.OneMR.output.path",
                         new File(tmpFolder.newFolder(), "OneMROutput").getAbsolutePath());
    runtimeArguments.put("spark.AnotherSpark.output.path",
                         new File(tmpFolder.newFolder(), "AnotherSparkOutput").getAbsolutePath());

    runtimeArguments.put("mapreduce.*.processing.time", "1HR");

    runtimeArguments.put("dataset.Purchase.cache.seconds", "30");
    runtimeArguments.put("dataset.UserProfile.schema.property", "constant");
    runtimeArguments.put("dataset.unknown.dataset", "false");
    runtimeArguments.put("dataset.*.read.timeout", "60");

    setAndTestRuntimeArgs(programId, runtimeArguments);

    // Start the workflow
    startProgram(programId, 200);

    verifyAndGetWorkflowRuns(programId, "completed");
  }

  @Test
  public void testWorkflowSchedules() throws Exception {
    // Steps for the test:
    // 1. Deploy the app
    // 2. Verify the schedules
    // 3. Verify the history after waiting a while
    // 4. Suspend the schedule
    // 5. Verify there are no runs after the suspend by looking at the history
    // 6. Resume the schedule
    // 7. Verify there are runs after the resume by looking at the history

    // deploy app with schedule in namespace 2
    HttpResponse response = deploy(AppWithSchedule.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Id.Program programId = Id.Program.from(TEST_NAMESPACE2, APP_WITH_SCHEDULE_APP_NAME, ProgramType.WORKFLOW,
                                           APP_WITH_SCHEDULE_WORKFLOW_NAME);

    Map<String, String> runtimeArguments = Maps.newHashMap();
    runtimeArguments.put("someKey", "someWorkflowValue");
    runtimeArguments.put("workflowKey", "workflowValue");

    setAndTestRuntimeArgs(programId, runtimeArguments);

    Assert.assertEquals(200,
                        resumeSchedule(TEST_NAMESPACE2, APP_WITH_SCHEDULE_APP_NAME, APP_WITH_SCHEDULE_SCHEDULE_NAME));

    // get schedules
    List<ScheduleSpecification> schedules = getSchedules(TEST_NAMESPACE2, APP_WITH_SCHEDULE_APP_NAME,
                                                         APP_WITH_SCHEDULE_WORKFLOW_NAME);
    Assert.assertEquals(1, schedules.size());
    String scheduleName = schedules.get(0).getSchedule().getName();
    Assert.assertNotNull(scheduleName);
    Assert.assertFalse(scheduleName.isEmpty());

    long current = System.currentTimeMillis();
    Long nextRunTime = getNextScheduledRunTime(programId, scheduleName);
    Assert.assertNotNull(nextRunTime);
    Assert.assertTrue(nextRunTime > current);

    verifyAndGetWorkflowRuns(programId, "completed");

    //Check schedule status
    String statusURL = getStatusURL(TEST_NAMESPACE2, APP_WITH_SCHEDULE_APP_NAME, scheduleName);
    scheduleStatusCheck(5, statusURL, "SCHEDULED");

    Assert.assertEquals(200, suspendSchedule(TEST_NAMESPACE2, APP_WITH_SCHEDULE_APP_NAME, scheduleName));
    //check paused state
    scheduleStatusCheck(5, statusURL, "SUSPENDED");

    TimeUnit.SECONDS.sleep(2); //wait till any running jobs just before suspend call completes.

    int workflowRuns = verifyAndGetWorkflowRuns(programId, "completed").size();

    //Sleep for some time and verify there are no more scheduled jobs after the suspend.
    TimeUnit.SECONDS.sleep(10);

    int workflowRunsAfterSuspend = verifyAndGetWorkflowRuns(programId, "completed").size();
    Assert.assertEquals(workflowRuns, workflowRunsAfterSuspend);

    Assert.assertEquals(200, resumeSchedule(TEST_NAMESPACE2, APP_WITH_SCHEDULE_APP_NAME, scheduleName));

    verifyAndGetWorkflowRuns(programId, "completed", workflowRunsAfterSuspend);

    //check scheduled state
    scheduleStatusCheck(5, statusURL, "SCHEDULED");

    //Check status of a non existing schedule
    String invalid = getStatusURL(TEST_NAMESPACE2, APP_WITH_SCHEDULE_APP_NAME, "invalid");
    scheduleStatusCheck(5, invalid, "NOT_FOUND");

    Assert.assertEquals(200, suspendSchedule(TEST_NAMESPACE2, APP_WITH_SCHEDULE_APP_NAME, scheduleName));

    //check paused state
    scheduleStatusCheck(5, statusURL, "SUSPENDED");

    //Schedule operations using invalid namespace
    String inValidNamespaceURL = getStatusURL(TEST_NAMESPACE1, APP_WITH_SCHEDULE_APP_NAME, scheduleName);
    scheduleStatusCheck(5, inValidNamespaceURL, "NOT_FOUND");
    Assert.assertEquals(404, suspendSchedule(TEST_NAMESPACE1, APP_WITH_SCHEDULE_APP_NAME, scheduleName));
    Assert.assertEquals(404, resumeSchedule(TEST_NAMESPACE1, APP_WITH_SCHEDULE_APP_NAME, scheduleName));

    TimeUnit.SECONDS.sleep(2); //wait till any running jobs just before suspend call completes.
  }

  @Test
  public void testStreamSizeSchedules() throws Exception {
    // Steps for the test:
    // 1. Deploy the app
    // 2. Verify the schedules
    // 3. Ingest data in the stream
    // 4. Verify the history after waiting a while
    // 5. Suspend the schedule
    // 6. Ingest data in the stream
    // 7. Verify there are no runs after the suspend by looking at the history
    // 8. Resume the schedule
    // 9. Verify there are runs after the resume by looking at the history

    StringBuilder longStringBuilder = new StringBuilder();
    for (int i = 0; i < 10000; i++) {
      longStringBuilder.append("dddddddddd");
    }
    String longString = longStringBuilder.toString();

    // deploy app with schedule in namespace 2
    HttpResponse response = deploy(AppWithStreamSizeSchedule.class, Constants.Gateway.API_VERSION_3_TOKEN,
                                   TEST_NAMESPACE2);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Assert.assertEquals(200, resumeSchedule(TEST_NAMESPACE2, APP_WITH_STREAM_SCHEDULE_APP_NAME,
                                            APP_WITH_STREAM_SCHEDULE_SCHEDULE_NAME_1));
    Assert.assertEquals(200, resumeSchedule(TEST_NAMESPACE2, APP_WITH_STREAM_SCHEDULE_APP_NAME,
                                            APP_WITH_STREAM_SCHEDULE_SCHEDULE_NAME_2));

    // get schedules
    List<ScheduleSpecification> schedules = getSchedules(TEST_NAMESPACE2, APP_WITH_STREAM_SCHEDULE_APP_NAME,
                                                         APP_WITH_STREAM_SCHEDULE_WORKFLOW_NAME);
    Assert.assertEquals(2, schedules.size());
    String scheduleName1 = schedules.get(0).getSchedule().getName();
    String scheduleName2 = schedules.get(1).getSchedule().getName();
    Assert.assertNotNull(scheduleName1);
    Assert.assertFalse(scheduleName1.isEmpty());

    // Change notification threshold for stream
    response = doPut(String.format("/v3/namespaces/%s/streams/%s/properties", TEST_NAMESPACE2,
                                   APP_WITH_STREAM_SCHEDULE_STREAM_NAME),
                     "{'notification.threshold.mb': 1}");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = doGet(String.format("/v3/namespaces/%s/streams/%s", TEST_NAMESPACE2,
                                   APP_WITH_STREAM_SCHEDULE_STREAM_NAME));
    String json = EntityUtils.toString(response.getEntity());
    StreamProperties properties = new Gson().fromJson(json, StreamProperties.class);
    Assert.assertEquals(1, properties.getNotificationThresholdMB().intValue());

    // Ingest over 1MB of data in stream
    for (int i = 0; i < 12; ++i) {
      response = doPost(String.format("/v3/namespaces/%s/streams/%s", TEST_NAMESPACE2,
                                      APP_WITH_STREAM_SCHEDULE_STREAM_NAME),
                        longString);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    }

    TimeUnit.SECONDS.sleep(10);
    String runsURL = getRunsUrl(TEST_NAMESPACE2, APP_WITH_STREAM_SCHEDULE_APP_NAME,
                                APP_WITH_STREAM_SCHEDULE_WORKFLOW_NAME,
                                "completed");
    scheduleHistoryRuns(5, runsURL, 0);

    //Check schedule status
    String statusURL1 = getStatusURL(TEST_NAMESPACE2, APP_WITH_STREAM_SCHEDULE_APP_NAME, scheduleName1);
    String statusURL2 = getStatusURL(TEST_NAMESPACE2, APP_WITH_STREAM_SCHEDULE_APP_NAME, scheduleName2);
    scheduleStatusCheck(5, statusURL1, "SCHEDULED");
    scheduleStatusCheck(5, statusURL2, "SCHEDULED");

    Assert.assertEquals(200, suspendSchedule(TEST_NAMESPACE2, APP_WITH_STREAM_SCHEDULE_APP_NAME,
                                             scheduleName1));
    Assert.assertEquals(200, suspendSchedule(TEST_NAMESPACE2, APP_WITH_STREAM_SCHEDULE_APP_NAME,
                                             scheduleName2));
    //check paused state
    scheduleStatusCheck(5, statusURL1, "SUSPENDED");
    scheduleStatusCheck(5, statusURL2, "SUSPENDED");

    TimeUnit.SECONDS.sleep(2); //wait till any running jobs just before suspend call completes.

    int workflowRuns = getRuns(runsURL);

    // Sleep for some time and verify there are no more scheduled jobs after the suspend.
    for (int i = 0; i < 12; ++i) {
      response = doPost(String.format("/v3/namespaces/%s/streams/%s", TEST_NAMESPACE2,
                                      APP_WITH_STREAM_SCHEDULE_STREAM_NAME),
                        longString);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    }
    TimeUnit.SECONDS.sleep(5);

    int workflowRunsAfterSuspend = getRuns(runsURL);
    Assert.assertEquals(workflowRuns, workflowRunsAfterSuspend);

    Assert.assertEquals(200, resumeSchedule(TEST_NAMESPACE2, APP_WITH_STREAM_SCHEDULE_APP_NAME,
                                            scheduleName1));

    scheduleHistoryRuns(5, runsURL, workflowRunsAfterSuspend);

    //check scheduled state
    scheduleStatusCheck(5, statusURL1, "SCHEDULED");

    //Check status of a non existing schedule
    String invalid = getStatusURL(TEST_NAMESPACE2, APP_WITH_STREAM_SCHEDULE_APP_NAME, "invalid");
    scheduleStatusCheck(5, invalid, "NOT_FOUND");

    Assert.assertEquals(200, suspendSchedule(TEST_NAMESPACE2, APP_WITH_STREAM_SCHEDULE_APP_NAME, scheduleName1));

    //check paused state
    scheduleStatusCheck(5, statusURL1, "SUSPENDED");

    //Schedule operations using invalid namespace
    String inValidNamespaceURL = getStatusURL(TEST_NAMESPACE1, APP_WITH_STREAM_SCHEDULE_APP_NAME, scheduleName1);
    scheduleStatusCheck(5, inValidNamespaceURL, "NOT_FOUND");
    Assert.assertEquals(404, suspendSchedule(TEST_NAMESPACE1, APP_WITH_STREAM_SCHEDULE_APP_NAME, scheduleName1));
    Assert.assertEquals(404, resumeSchedule(TEST_NAMESPACE1, APP_WITH_STREAM_SCHEDULE_APP_NAME, scheduleName1));

    TimeUnit.SECONDS.sleep(2); //wait till any running jobs just before suspend call completes.
  }

  @Test
  public void testWorkflowRuns() throws Exception {
    HttpResponse response = deploy(WorkflowAppWithErrorRuns.class, Constants.Gateway.API_VERSION_3_TOKEN,
                                   TEST_NAMESPACE2);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Assert.assertEquals(200, resumeSchedule(TEST_NAMESPACE2, WORKFLOW_APP_WITH_ERROR_RUNS,
                                            WORKFLOW_WITH_ERROR_RUNS_SCHEDULE));

    Id.Program programId = Id.Program.from(TEST_NAMESPACE2, WORKFLOW_APP_WITH_ERROR_RUNS, ProgramType.WORKFLOW,
                                           WORKFLOW_WITH_ERROR_RUNS);

    verifyAndGetWorkflowRuns(programId, "completed");

    Map<String, String> propMap = ImmutableMap.of("ThrowError", "true");
    PreferencesStore store = getInjector().getInstance(PreferencesStore.class);
    store.setProperties(TEST_NAMESPACE2, WORKFLOW_APP_WITH_ERROR_RUNS, ProgramType.WORKFLOW.getCategoryName(),
                        WORKFLOW_WITH_ERROR_RUNS, propMap);

    verifyAndGetWorkflowRuns(programId, "failed");

    Assert.assertEquals(200, suspendSchedule(TEST_NAMESPACE2, WORKFLOW_APP_WITH_ERROR_RUNS,
                                             WORKFLOW_WITH_ERROR_RUNS_SCHEDULE));
  }
}
