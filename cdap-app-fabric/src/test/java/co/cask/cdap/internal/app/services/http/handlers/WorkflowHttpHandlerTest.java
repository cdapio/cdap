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
import co.cask.cdap.ConditionalWorkflowApp;
import co.cask.cdap.PauseResumeWorklowApp;
import co.cask.cdap.WorkflowAppWithErrorRuns;
import co.cask.cdap.WorkflowAppWithFork;
import co.cask.cdap.WorkflowAppWithScopedParameters;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.gateway.handlers.WorkflowHttpHandler;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.ScheduledRuntime;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.proto.codec.ScheduleSpecificationCodec;
import co.cask.cdap.proto.codec.WorkflowActionSpecificationCodec;
import co.cask.cdap.test.XSlowTests;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link WorkflowHttpHandler}
 */
public class WorkflowHttpHandlerTest  extends AppFabricTestBase {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(ScheduleSpecification.class, new ScheduleSpecificationCodec())
    .registerTypeAdapter(WorkflowActionSpecification.class, new WorkflowActionSpecificationCodec())
    .create();

  protected static final Type LIST_WORKFLOWACTIONNODE_TYPE = new TypeToken<List<WorkflowActionNode>>()
  { }.getType();

  private String getRunsUrl(String namespace, String appName, String workflow, String status) {
    String runsUrl = String.format("apps/%s/workflows/%s/runs?status=%s", appName, workflow, status);
    return getVersionedAPIPath(runsUrl, Constants.Gateway.API_VERSION_3_TOKEN, namespace);
  }

  private void verifyRunningProgramCount(final Id.Program program, final String runId, final int expected)
    throws Exception {
    Tasks.waitFor(expected, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return runningProgramCount(program, runId);
      }
    }, 10, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
  }

  private Integer runningProgramCount(Id.Program program, String runId) throws Exception {
    String path = String.format("apps/%s/workflows/%s/%s/current", program.getApplicationId(), program.getId(), runId);
    HttpResponse response = doGet(getVersionedAPIPath(path, program.getNamespaceId()));
    if (response.getStatusLine().getStatusCode() == 200) {
      String json = EntityUtils.toString(response.getEntity());
      List<WorkflowActionNode> output = GSON.fromJson(json, LIST_WORKFLOWACTIONNODE_TYPE);
      return output.size();
    }
    return null;
  }

  private void suspendWorkflow(Id.Program program, String runId, int expectedStatusCode) throws Exception {
    String path = String.format("apps/%s/workflows/%s/runs/%s/suspend", program.getApplicationId(), program.getId(),
                                runId);
    HttpResponse response = doPost(getVersionedAPIPath(path, Constants.Gateway.API_VERSION_3_TOKEN,
                                                       program.getNamespaceId()));
    Assert.assertEquals(expectedStatusCode, response.getStatusLine().getStatusCode());
  }

  private void setAndTestRuntimeArgs(Id.Program programId, Map<String, String> args) throws Exception {
    HttpResponse response;
    String argString = GSON.toJson(args, new TypeToken<Map<String, String>>() {
    }.getType());
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
    String path = String.format("apps/%s/workflows/%s/runs/%s/resume", program.getApplicationId(), program.getId(),
                                runId);
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

  private List<ScheduledRuntime> getScheduledRunTime(Id.Program program, String schedule,
                                                     String prevOrNext) throws Exception {
    String nextRunTimeUrl = String.format("apps/%s/workflows/%s/%s", program.getApplicationId(), program.getId(),
                                          prevOrNext);
    String versionedUrl = getVersionedAPIPath(nextRunTimeUrl, Constants.Gateway.API_VERSION_3_TOKEN,
                                              program.getNamespaceId());
    HttpResponse response = doGet(versionedUrl);
    return readResponse(response, new TypeToken<List<ScheduledRuntime>>() {
    }.getType());
  }

  private String getStatusURL(String namespace, String appName, String schedule) throws Exception {
    String statusURL = String.format("apps/%s/schedules/%s/status", appName, schedule);
    return getVersionedAPIPath(statusURL, Constants.Gateway.API_VERSION_3_TOKEN, namespace);
  }

  @Test
  public void testWorkflowPauseResume() throws Exception {
    String pauseResumeWorkflowApp = "PauseResumeWorkflowApp";
    String pauseResumeWorkflow = "PauseResumeWorkflow";

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

    Id.Program programId = Id.Program.from(TEST_NAMESPACE2, pauseResumeWorkflowApp, ProgramType.WORKFLOW,
                                           pauseResumeWorkflow);

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
    List<RunRecord> historyRuns = getProgramRuns(programId, "running");
    Assert.assertTrue(historyRuns.size() == 1);
    String runId = historyRuns.get(0).getPid();

    while (!firstSimpleActionFile.exists()) {
      TimeUnit.MILLISECONDS.sleep(50);
    }

    // Only one Workflow node should be running
    verifyRunningProgramCount(programId, runId, 1);

    // Suspend the Workflow
    suspendWorkflow(programId, runId, 200);

    // Workflow status hould be SUSPENDED
    waitState(programId, "SUSPENDED");

    // Meta store information for this Workflow should reflect suspended run
    verifyProgramRuns(programId, "suspended");

    // Suspending the already suspended Workflow should give CONFLICT
    suspendWorkflow(programId, runId, 409);

    // Signal the FirstSimpleAction in the Workflow to continue
    firstSimpleActionDoneFile.createNewFile();

    // Even if the Workflow is suspended, currently executing action will complete and currently running nodes
    // should be zero
    verifyRunningProgramCount(programId, runId, 0);

    // Verify that Workflow is still suspended
    verifyProgramRuns(programId, "suspended");

    // Resume the execution of the Workflow
    resumeWorkflow(programId, runId, 200);

    // Workflow should be running
    waitState(programId, "RUNNING");

    verifyProgramRuns(programId, "running");

    // Resume on already running Workflow should give conflict
    resumeWorkflow(programId, runId, 409);

    // Wait till fork execution in the Workflow starts
    while (!(forkedSimpleActionFile.exists() && anotherForkedSimpleActionFile.exists())) {
      TimeUnit.MILLISECONDS.sleep(50);
    }

    // Workflow should have 2 nodes running because of the fork
    verifyRunningProgramCount(programId, runId, 2);

    // Suspend the Workflow
    suspendWorkflow(programId, runId, 200);

    // Status of the Workflow should be suspended
    waitState(programId, "SUSPENDED");

    // Store should reflect the suspended status of the Workflow
    verifyProgramRuns(programId, "suspended");

    // Allow currently executing actions to complete
    forkedSimpleActionDoneFile.createNewFile();
    anotherForkedSimpleActionDoneFile.createNewFile();

    // Workflow should have zero actions running
    verifyRunningProgramCount(programId, runId, 0);

    verifyProgramRuns(programId, "suspended");

    Assert.assertTrue(!lastSimpleActionFile.exists());

    resumeWorkflow(programId, runId, 200);

    waitState(programId, "RUNNING");

    while (!lastSimpleActionFile.exists()) {
      TimeUnit.SECONDS.sleep(1);
    }

    verifyRunningProgramCount(programId, runId, 1);

    lastSimpleActionDoneFile.createNewFile();

    verifyProgramRuns(programId, "completed");

    waitState(programId, "STOPPED");
  }

  @Category(XSlowTests.class)
  @Test
  public void testMultipleWorkflowInstances() throws Exception {
    String appWithConcurrentWorkflow = "ConcurrentWorkflowApp";
    String appWithConcurrentWorkflowSchedule1 = "concurrentWorkflowSchedule1";
    String appWithConcurrentWorkflowSchedule2 = "concurrentWorkflowSchedule2";
    String concurrentWorkflowName = "ConcurrentWorkflow";

    // Files used to synchronize between this test and workflow execution
    File schedule1File = new File(tmpFolder.newFolder() + "/concurrentWorkflowSchedule1.file");
    File schedule2File = new File(tmpFolder.newFolder() + "/concurrentWorkflowSchedule2.file");
    File simpleActionDoneFile = new File(tmpFolder.newFolder() + "/simpleaction.file.done");

    // create app in default namespace so that v2 and v3 api can be tested in the same test
    String defaultNamespace = "default";
    HttpResponse response = deploy(ConcurrentWorkflowApp.class, Constants.Gateway.API_VERSION_3_TOKEN,
                                   defaultNamespace);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Id.Program programId = Id.Program.from(defaultNamespace, appWithConcurrentWorkflow, ProgramType.WORKFLOW,
                                           concurrentWorkflowName);


    Map<String, String> propMap = ImmutableMap.of(ProgramOptionConstants.CONCURRENT_RUNS_ENABLED, "true",
                                                           "concurrentWorkflowSchedule1.file",
                                                           schedule1File.getAbsolutePath(),
                                                           "concurrentWorkflowSchedule2.file",
                                                           schedule2File.getAbsolutePath(),
                                                           "done.file", simpleActionDoneFile.getAbsolutePath());

    PreferencesStore store = getInjector().getInstance(PreferencesStore.class);
    store.setProperties(defaultNamespace, appWithConcurrentWorkflow, ProgramType.WORKFLOW.getCategoryName(),
                        concurrentWorkflowName, propMap);

    Assert.assertEquals(200, resumeSchedule(defaultNamespace, appWithConcurrentWorkflow,
                                            appWithConcurrentWorkflowSchedule1));
    Assert.assertEquals(200, resumeSchedule(defaultNamespace, appWithConcurrentWorkflow,
                                            appWithConcurrentWorkflowSchedule2));

    while (!(schedule1File.exists() && schedule2File.exists())) {
      TimeUnit.MILLISECONDS.sleep(50);
    }

    List<RunRecord> historyRuns = getProgramRuns(programId, "running");
    Assert.assertTrue(historyRuns.size() >= 2);

    // Suspend ConcurrentWorkflow schedules
    List<ScheduleSpecification> schedules = getSchedules(defaultNamespace, appWithConcurrentWorkflow,
                                                         concurrentWorkflowName);

    for (ScheduleSpecification spec : schedules) {
      Assert.assertEquals(200, suspendSchedule(defaultNamespace, appWithConcurrentWorkflow,
                                               spec.getSchedule().getName()));
    }

    response = getWorkflowCurrentStatus(programId, historyRuns.get(0).getPid());
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String json = EntityUtils.toString(response.getEntity());
    List<WorkflowActionNode> nodes = GSON.fromJson(json, LIST_WORKFLOWACTIONNODE_TYPE);
    Assert.assertEquals(1, nodes.size());
    Assert.assertEquals("SimpleAction", nodes.get(0).getProgram().getProgramName());

    response = getWorkflowCurrentStatus(programId, historyRuns.get(1).getPid());
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    json = EntityUtils.toString(response.getEntity());
    nodes = GSON.fromJson(json, LIST_WORKFLOWACTIONNODE_TYPE);
    Assert.assertEquals(1, nodes.size());
    Assert.assertEquals("SimpleAction", nodes.get(0).getProgram().getProgramName());

    simpleActionDoneFile.createNewFile();

    // delete the application
    String deleteURL = getVersionedAPIPath("apps/" + appWithConcurrentWorkflow,
                                           Constants.Gateway.API_VERSION_3_TOKEN, defaultNamespace);
    deleteApplication(60, deleteURL, 200);
  }

  private void verifyFileExists(final List<File> fileList)
    throws Exception {
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        for (File file : fileList) {
          if (!file.exists()) {
            return false;
          }
        }
        return true;
      }
    }, 180, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testWorkflowForkApp() throws Exception {
    String workflowAppWithFork = "WorkflowAppWithFork";
    String workflowWithFork = "WorkflowWithFork";

    Map<String, String> runtimeArgs = Maps.newHashMap();

    // Files used to synchronize between this test and workflow execution
    File firstSimpleActionFile = new File(tmpFolder.newFolder() + "/firstsimpleaction.file");
    File firstSimpleActionDoneFile = new File(tmpFolder.newFolder() + "/firstsimpleaction.file.done");
    runtimeArgs.put("first.simple.action.file", firstSimpleActionFile.getAbsolutePath());
    runtimeArgs.put("first.simple.action.donefile", firstSimpleActionDoneFile.getAbsolutePath());

    File oneSimpleActionFile = new File(tmpFolder.newFolder() + "/onesimpleaction.file");
    File oneSimpleActionDoneFile = new File(tmpFolder.newFolder() + "/onesimpleaction.file.done");
    runtimeArgs.put("one.simple.action.file", oneSimpleActionFile.getAbsolutePath());
    runtimeArgs.put("one.simple.action.donefile", oneSimpleActionDoneFile.getAbsolutePath());

    File anotherSimpleActionFile = new File(tmpFolder.newFolder() + "/anothersimpleaction.file");
    File anotherSimpleActionDoneFile = new File(tmpFolder.newFolder() + "/anothersimpleaction.file.done");
    runtimeArgs.put("another.simple.action.file", anotherSimpleActionFile.getAbsolutePath());
    runtimeArgs.put("another.simple.action.donefile", anotherSimpleActionDoneFile.getAbsolutePath());

    HttpResponse response = deploy(WorkflowAppWithFork.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Id.Program programId = Id.Program.from(TEST_NAMESPACE2, workflowAppWithFork, ProgramType.WORKFLOW,
                                           workflowWithFork);

    setAndTestRuntimeArgs(programId, runtimeArgs);

    // Start a Workflow
    startProgram(programId, 200);

    // Workflow should be running
    waitState(programId, "RUNNING");

    // Get the currently running RunRecord for the Workflow
    List<RunRecord> historyRuns = getProgramRuns(programId, "running");
    Assert.assertTrue(historyRuns.size() == 1);
    RunRecord record = historyRuns.get(0);
    String runId = record.getPid();

    // Wait till first action in the Workflow starts executing
    verifyFileExists(Lists.newArrayList(firstSimpleActionFile));

    verifyRunningProgramCount(programId, runId, 1);

    // Stop the Workflow
    stopProgram(programId, 200);

    // Workflow run record should be marked 'killed'
    verifyProgramRuns(programId, "killed");

    // Delete the asset created in the previous run
    firstSimpleActionFile.delete();

    // Start the Workflow again
    startProgram(programId, 200);

    // Workflow should be running
    waitState(programId, "RUNNING");

    // Get the currently running RunRecord for the Workflow
    historyRuns = getProgramRuns(programId, "running");
    Assert.assertTrue(historyRuns.size() == 1);
    record = historyRuns.get(0);
    Assert.assertTrue(!runId.equals(record.getPid()));

    // Store the new RunId
    runId = record.getPid();

    // Wait till first action in the Workflow starts executing
    verifyFileExists(Lists.newArrayList(firstSimpleActionFile));

    verifyRunningProgramCount(programId, runId, 1);

    // Signal the first action to continue
    firstSimpleActionDoneFile.createNewFile();

    // Wait till fork in the Workflow starts executing
    verifyFileExists(Lists.newArrayList(oneSimpleActionFile, anotherSimpleActionFile));

    // Two actions should be running in Workflow as a part of the fork
    verifyRunningProgramCount(programId, runId, 2);

    // Stop the program while in fork
    stopProgram(programId, 200);

    // Current endpoint would return 404
    response = getWorkflowCurrentStatus(programId, runId);
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    // Now there should be 2 RunRecord with status killed
    verifyProgramRuns(programId, "killed", 1);

    // Delete the assets generated in the previous run
    firstSimpleActionFile.delete();
    firstSimpleActionDoneFile.delete();
    oneSimpleActionFile.delete();
    anotherSimpleActionFile.delete();

    // Restart the run again
    startProgram(programId, 200);

    // Wait till the Workflow is running
    waitState(programId, "RUNNING");

    // Store the new RunRecord for the currently running run
    historyRuns = getProgramRuns(programId, "running");
    Assert.assertTrue(historyRuns.size() == 1);
    runId = historyRuns.get(0).getPid();

    // Wait till first action in the Workflow starts executing
    verifyFileExists(Lists.newArrayList(firstSimpleActionFile));

    verifyRunningProgramCount(programId, runId, 1);

    // Signal the first action to continue
    firstSimpleActionDoneFile.createNewFile();

    // Wait till fork in the Workflow starts executing
    verifyFileExists(Lists.newArrayList(oneSimpleActionFile, anotherSimpleActionFile));

    // Two actions should be running in Workflow as a part of the fork
    verifyRunningProgramCount(programId, runId, 2);

    // Signal the Workflow that execution can be continued
    oneSimpleActionDoneFile.createNewFile();
    anotherSimpleActionDoneFile.createNewFile();

    // Workflow should now have one completed run
    verifyProgramRuns(programId, "completed");
  }

  @Category(XSlowTests.class)
  @Test
  public void testWorkflowScopedArguments() throws Exception {
    String workflowAppWithScopedParameters = "WorkflowAppWithScopedParameters";
    String workflowAppWithScopedParameterWorkflow = "OneWorkflow";

    HttpResponse response = deploy(WorkflowAppWithScopedParameters.class, Constants.Gateway.API_VERSION_3_TOKEN,
                                   TEST_NAMESPACE2);

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Id.Program programId = Id.Program.from(TEST_NAMESPACE2, workflowAppWithScopedParameters, ProgramType.WORKFLOW,
                                           workflowAppWithScopedParameterWorkflow);

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

    verifyProgramRuns(programId, "completed");

    List<RunRecord> workflowHistoryRuns = getProgramRuns(programId, "completed");

    Id.Program mr1ProgramId = Id.Program.from(TEST_NAMESPACE2, workflowAppWithScopedParameters, ProgramType.MAPREDUCE,
                                              "OneMR");

    List<RunRecord> oneMRHistoryRuns = getProgramRuns(mr1ProgramId, "completed");

    Id.Program mr2ProgramId = Id.Program.from(TEST_NAMESPACE2, workflowAppWithScopedParameters, ProgramType.MAPREDUCE,
                                              "AnotherMR");

    List<RunRecord> anotherMRHistoryRuns = getProgramRuns(mr2ProgramId, "completed");

    Id.Program spark1ProgramId = Id.Program.from(TEST_NAMESPACE2, workflowAppWithScopedParameters,
                                                 ProgramType.SPARK, "OneSpark");

    List<RunRecord> oneSparkHistoryRuns = getProgramRuns(spark1ProgramId, "completed");

    Id.Program spark2ProgramId = Id.Program.from(TEST_NAMESPACE2, workflowAppWithScopedParameters, ProgramType.SPARK,
                                              "AnotherSpark");

    List<RunRecord> anotherSparkHistoryRuns = getProgramRuns(spark2ProgramId, "completed");


    Assert.assertEquals(1, workflowHistoryRuns.size());
    Assert.assertEquals(1, oneMRHistoryRuns.size());
    Assert.assertEquals(1, anotherMRHistoryRuns.size());
    Assert.assertEquals(1, oneSparkHistoryRuns.size());
    Assert.assertEquals(1, anotherSparkHistoryRuns.size());

    Map<String, String> workflowRunRecordProperties = workflowHistoryRuns.get(0).getProperties();
    Map<String, String> oneMRRunRecordProperties = oneMRHistoryRuns.get(0).getProperties();
    Map<String, String> anotherMRRunRecordProperties = anotherMRHistoryRuns.get(0).getProperties();
    Map<String, String> oneSparkRunRecordProperties = oneSparkHistoryRuns.get(0).getProperties();
    Map<String, String> anotherSparkRunRecordProperties = anotherSparkHistoryRuns.get(0).getProperties();

    Assert.assertNotNull(oneMRRunRecordProperties.get("workflowrunid"));
    Assert.assertEquals(workflowHistoryRuns.get(0).getPid(), oneMRRunRecordProperties.get("workflowrunid"));

    Assert.assertNotNull(anotherMRRunRecordProperties.get("workflowrunid"));
    Assert.assertEquals(workflowHistoryRuns.get(0).getPid(), anotherMRRunRecordProperties.get("workflowrunid"));

    Assert.assertNotNull(oneSparkRunRecordProperties.get("workflowrunid"));
    Assert.assertEquals(workflowHistoryRuns.get(0).getPid(), oneSparkRunRecordProperties.get("workflowrunid"));

    Assert.assertNotNull(anotherSparkRunRecordProperties.get("workflowrunid"));
    Assert.assertEquals(workflowHistoryRuns.get(0).getPid(), anotherSparkRunRecordProperties.get("workflowrunid"));

    Assert.assertEquals(workflowRunRecordProperties.get("0"), oneMRHistoryRuns.get(0).getPid());
    Assert.assertEquals(workflowRunRecordProperties.get("1"), oneSparkHistoryRuns.get(0).getPid());
    Assert.assertEquals(workflowRunRecordProperties.get("2"), anotherMRHistoryRuns.get(0).getPid());
    Assert.assertEquals(workflowRunRecordProperties.get("3"), anotherSparkHistoryRuns.get(0).getPid());
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

    String appName = "AppWithSchedule";
    String workflowName = "SampleWorkflow";
    String sampleSchedule = "SampleSchedule";

    // deploy app with schedule in namespace 2
    HttpResponse response = deploy(AppWithSchedule.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Id.Program programId = Id.Program.from(TEST_NAMESPACE2, appName, ProgramType.WORKFLOW, workflowName);

    Map<String, String> runtimeArguments = Maps.newHashMap();
    runtimeArguments.put("someKey", "someWorkflowValue");
    runtimeArguments.put("workflowKey", "workflowValue");

    setAndTestRuntimeArgs(programId, runtimeArguments);

    // get schedules
    List<ScheduleSpecification> schedules = getSchedules(TEST_NAMESPACE2, appName, workflowName);
    Assert.assertEquals(1, schedules.size());
    String scheduleName = schedules.get(0).getSchedule().getName();
    Assert.assertNotNull(scheduleName);
    Assert.assertFalse(scheduleName.isEmpty());

    // TODO [CDAP-2327] Sagar Investigate why following check fails sometimes. Mostly test case issue.
    // List<ScheduledRuntime> previousRuntimes = getScheduledRunTime(programId, scheduleName, "previousruntime");
    // Assert.assertTrue(previousRuntimes.size() == 0);

    Assert.assertEquals(200, resumeSchedule(TEST_NAMESPACE2, appName, sampleSchedule));

    long current = System.currentTimeMillis();

    List<ScheduledRuntime> runtimes = getScheduledRunTime(programId, scheduleName, "nextruntime");
    String id = runtimes.get(0).getId();
    Assert.assertTrue(id.contains(scheduleName));
    Long nextRunTime = runtimes.get(0).getTime();
    Assert.assertTrue(nextRunTime > current);

    verifyProgramRuns(programId, "completed");

    List<ScheduledRuntime> previousRuntimes = getScheduledRunTime(programId, scheduleName, "previousruntime");
    Assert.assertEquals(1, previousRuntimes.size());

    //Check schedule status
    String statusURL = getStatusURL(TEST_NAMESPACE2, appName, scheduleName);
    scheduleStatusCheck(5, statusURL, "SCHEDULED");

    Assert.assertEquals(200, suspendSchedule(TEST_NAMESPACE2, appName, scheduleName));
    //check paused state
    scheduleStatusCheck(5, statusURL, "SUSPENDED");

    TimeUnit.SECONDS.sleep(2); //wait till any running jobs just before suspend call completes.

    int workflowRuns = getProgramRuns(programId, "completed").size();

    //Sleep for some time and verify there are no more scheduled jobs after the suspend.
    TimeUnit.SECONDS.sleep(10);

    int workflowRunsAfterSuspend = getProgramRuns(programId, "completed").size();
    Assert.assertEquals(workflowRuns, workflowRunsAfterSuspend);

    Assert.assertEquals(200, resumeSchedule(TEST_NAMESPACE2, appName, scheduleName));

    verifyProgramRuns(programId, "completed", workflowRunsAfterSuspend);

    //check scheduled state
    scheduleStatusCheck(5, statusURL, "SCHEDULED");

    //Check status of a non existing schedule
    String invalid = getStatusURL(TEST_NAMESPACE2, appName, "invalid");
    scheduleStatusCheck(5, invalid, "NOT_FOUND");

    Assert.assertEquals(200, suspendSchedule(TEST_NAMESPACE2, appName, scheduleName));

    //check paused state
    scheduleStatusCheck(5, statusURL, "SUSPENDED");

    //Schedule operations using invalid namespace
    String inValidNamespaceURL = getStatusURL(TEST_NAMESPACE1, appName, scheduleName);
    scheduleStatusCheck(5, inValidNamespaceURL, "NOT_FOUND");
    Assert.assertEquals(404, suspendSchedule(TEST_NAMESPACE1, appName, scheduleName));
    Assert.assertEquals(404, resumeSchedule(TEST_NAMESPACE1, appName, scheduleName));

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

    String appName = "AppWithStreamSizeSchedule";
    String sampleSchedule1 = "SampleSchedule1";
    String sampleSchedule2 = "SampleSchedule2";
    String workflowName = "SampleWorkflow";
    String streamName = "stream";

    StringBuilder longStringBuilder = new StringBuilder();
    for (int i = 0; i < 10000; i++) {
      longStringBuilder.append("dddddddddd");
    }
    String longString = longStringBuilder.toString();

    // deploy app with schedule in namespace 2
    HttpResponse response = deploy(AppWithStreamSizeSchedule.class, Constants.Gateway.API_VERSION_3_TOKEN,
                                   TEST_NAMESPACE2);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Assert.assertEquals(200, resumeSchedule(TEST_NAMESPACE2, appName, sampleSchedule1));
    Assert.assertEquals(200, resumeSchedule(TEST_NAMESPACE2, appName, sampleSchedule2));

    // get schedules
    List<ScheduleSpecification> schedules = getSchedules(TEST_NAMESPACE2, appName, workflowName);
    Assert.assertEquals(2, schedules.size());
    String scheduleName1 = schedules.get(0).getSchedule().getName();
    String scheduleName2 = schedules.get(1).getSchedule().getName();
    Assert.assertNotNull(scheduleName1);
    Assert.assertFalse(scheduleName1.isEmpty());

    // Change notification threshold for stream
    response = doPut(String.format("/v3/namespaces/%s/streams/%s/properties", TEST_NAMESPACE2, streamName),
                     "{'notification.threshold.mb': 1}");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = doGet(String.format("/v3/namespaces/%s/streams/%s", TEST_NAMESPACE2, streamName));
    String json = EntityUtils.toString(response.getEntity());
    StreamProperties properties = new Gson().fromJson(json, StreamProperties.class);
    Assert.assertEquals(1, properties.getNotificationThresholdMB().intValue());

    // Ingest over 1MB of data in stream
    for (int i = 0; i < 12; ++i) {
      response = doPost(String.format("/v3/namespaces/%s/streams/%s", TEST_NAMESPACE2, streamName), longString);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    }

    TimeUnit.SECONDS.sleep(10);
    String runsURL = getRunsUrl(TEST_NAMESPACE2, appName, workflowName, "completed");
    scheduleHistoryRuns(5, runsURL, 0);

    //Check schedule status
    String statusURL1 = getStatusURL(TEST_NAMESPACE2, appName, scheduleName1);
    String statusURL2 = getStatusURL(TEST_NAMESPACE2, appName, scheduleName2);
    scheduleStatusCheck(5, statusURL1, "SCHEDULED");
    scheduleStatusCheck(5, statusURL2, "SCHEDULED");

    Assert.assertEquals(200, suspendSchedule(TEST_NAMESPACE2, appName, scheduleName1));
    Assert.assertEquals(200, suspendSchedule(TEST_NAMESPACE2, appName, scheduleName2));
    //check paused state
    scheduleStatusCheck(5, statusURL1, "SUSPENDED");
    scheduleStatusCheck(5, statusURL2, "SUSPENDED");

    TimeUnit.SECONDS.sleep(2); //wait till any running jobs just before suspend call completes.

    int workflowRuns = getRuns(runsURL);

    // Sleep for some time and verify there are no more scheduled jobs after the suspend.
    for (int i = 0; i < 12; ++i) {
      response = doPost(String.format("/v3/namespaces/%s/streams/%s", TEST_NAMESPACE2, streamName), longString);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    }
    TimeUnit.SECONDS.sleep(5);

    int workflowRunsAfterSuspend = getRuns(runsURL);
    Assert.assertEquals(workflowRuns, workflowRunsAfterSuspend);

    Assert.assertEquals(200, resumeSchedule(TEST_NAMESPACE2, appName, scheduleName1));

    scheduleHistoryRuns(5, runsURL, workflowRunsAfterSuspend);

    //check scheduled state
    scheduleStatusCheck(5, statusURL1, "SCHEDULED");

    //Check status of a non existing schedule
    String invalid = getStatusURL(TEST_NAMESPACE2, appName, "invalid");
    scheduleStatusCheck(5, invalid, "NOT_FOUND");

    Assert.assertEquals(200, suspendSchedule(TEST_NAMESPACE2, appName, scheduleName1));

    //check paused state
    scheduleStatusCheck(5, statusURL1, "SUSPENDED");

    //Schedule operations using invalid namespace
    String inValidNamespaceURL = getStatusURL(TEST_NAMESPACE1, appName, scheduleName1);
    scheduleStatusCheck(5, inValidNamespaceURL, "NOT_FOUND");
    Assert.assertEquals(404, suspendSchedule(TEST_NAMESPACE1, appName, scheduleName1));
    Assert.assertEquals(404, resumeSchedule(TEST_NAMESPACE1, appName, scheduleName1));

    TimeUnit.SECONDS.sleep(2); //wait till any running jobs just before suspend call completes.
  }

  @Test
  public void testWorkflowRuns() throws Exception {
    String appName = "WorkflowAppWithErrorRuns";
    String workflowName = "WorkflowWithErrorRuns";
    String sampleSchedule = "SampleSchedule";

    HttpResponse response = deploy(WorkflowAppWithErrorRuns.class, Constants.Gateway.API_VERSION_3_TOKEN,
                                   TEST_NAMESPACE2);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Assert.assertEquals(200, resumeSchedule(TEST_NAMESPACE2, appName, sampleSchedule));

    Id.Program programId = Id.Program.from(TEST_NAMESPACE2, appName, ProgramType.WORKFLOW, workflowName);

    verifyProgramRuns(programId, "completed");

    Map<String, String> propMap = ImmutableMap.of("ThrowError", "true");
    PreferencesStore store = getInjector().getInstance(PreferencesStore.class);
    store.setProperties(TEST_NAMESPACE2, appName, ProgramType.WORKFLOW.getCategoryName(), workflowName, propMap);

    verifyProgramRuns(programId, "failed");

    Assert.assertEquals(200, suspendSchedule(TEST_NAMESPACE2, appName, sampleSchedule));
  }

  private String createConditionInput(String folderName, int numGoodRecords, int numBadRecords) throws IOException {
    File inputDir = tmpFolder.newFolder(folderName);

    File inputFile = new File(inputDir.getPath() + "/data.txt");
    BufferedWriter writer = new BufferedWriter(new FileWriter(inputFile));

    try {
      // dummy good records containing ":" separated fields
      for (int i = 0; i < numGoodRecords; i++) {
        writer.write("Afname:ALname:A:B");
        writer.newLine();
      }
      // dummy bad records in which fields are not separated by ":"
      for (int i = 0; i < numBadRecords; i++) {
        writer.write("Afname ALname A B");
        writer.newLine();
      }
    } finally {
      writer.close();
    }
    return inputDir.getAbsolutePath();
  }

  @Category(XSlowTests.class)
  @Test
  public void testWorkflowCondition() throws Exception {
    String conditionalWorkflowApp = "ConditionalWorkflowApp";
    String conditionalWorkflow = "ConditionalWorkflow";

    HttpResponse response = deploy(ConditionalWorkflowApp.class, Constants.Gateway.API_VERSION_3_TOKEN,
                                   TEST_NAMESPACE2);

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Id.Program programId = Id.Program.from(TEST_NAMESPACE2, conditionalWorkflowApp, ProgramType.WORKFLOW,
                                           conditionalWorkflow);

    Map<String, String> runtimeArguments = Maps.newHashMap();

    // Files used to synchronize between this test and workflow execution
    File ifForkOneActionFile = new File(tmpFolder.newFolder() + "/iffork_one.file");
    File ifForkOneActionDoneFile = new File(tmpFolder.newFolder() + "/iffork_one.file.done");
    runtimeArguments.put("iffork_one.simple.action.file", ifForkOneActionFile.getAbsolutePath());
    runtimeArguments.put("iffork_one.simple.action.donefile", ifForkOneActionDoneFile.getAbsolutePath());

    File ifForkAnotherActionFile = new File(tmpFolder.newFolder() + "/iffork_another.file");
    File ifForkAnotherActionDoneFile = new File(tmpFolder.newFolder() + "/iffork_another.file.done");
    runtimeArguments.put("iffork_another.simple.action.file", ifForkAnotherActionFile.getAbsolutePath());
    runtimeArguments.put("iffork_another.simple.action.donefile", ifForkAnotherActionDoneFile.getAbsolutePath());

    File elseForkOneActionFile = new File(tmpFolder.newFolder() + "/elsefork_one.file");
    File elseForkOneActionDoneFile = new File(tmpFolder.newFolder() + "/elsefork_one.file.done");
    runtimeArguments.put("elsefork_one.simple.action.file", elseForkOneActionFile.getAbsolutePath());
    runtimeArguments.put("elsefork_one.simple.action.donefile", elseForkOneActionDoneFile.getAbsolutePath());

    File elseForkAnotherActionFile = new File(tmpFolder.newFolder() + "/elsefork_another.file");
    File elseForkAnotherActionDoneFile = new File(tmpFolder.newFolder() + "/elsefork_another.file.done");
    runtimeArguments.put("elsefork_another.simple.action.file", elseForkAnotherActionFile.getAbsolutePath());
    runtimeArguments.put("elsefork_another.simple.action.donefile", elseForkAnotherActionDoneFile.getAbsolutePath());

    File elseForkThirdActionFile = new File(tmpFolder.newFolder() + "/elsefork_third.file");
    File elseForkThirdActionDoneFile = new File(tmpFolder.newFolder() + "/elsefork_third.file.done");
    runtimeArguments.put("elsefork_third.simple.action.file", elseForkThirdActionFile.getAbsolutePath());
    runtimeArguments.put("elsefork_third.simple.action.donefile", elseForkThirdActionDoneFile.getAbsolutePath());

    // create input data in which number of good records are lesser than the number of bad records
    runtimeArguments.put("inputPath", createConditionInput("ConditionProgramInput", 2, 12));
    runtimeArguments.put("outputPath", new File(tmpFolder.newFolder(), "ConditionProgramOutput").getAbsolutePath());
    setAndTestRuntimeArgs(programId, runtimeArguments);

    // Start the workflow
    startProgram(programId, 200);

    // Since the number of good records are lesser than the number of bad records,
    // 'else' branch of the condition will get executed.
    // Wait till the execution of the fork on the else branch starts
    while (!(elseForkOneActionFile.exists() &&
             elseForkAnotherActionFile.exists() &&
             elseForkThirdActionFile.exists())) {
      TimeUnit.MILLISECONDS.sleep(50);
    }

    // Get running program run
    List<RunRecord> historyRuns = getProgramRuns(programId, "running");
    Assert.assertTrue(historyRuns.size() == 1);
    String runId = historyRuns.get(0).getPid();

    // Since the fork on the else branch of condition has 3 parallel branches
    // there should be 3 programs currently running
    verifyRunningProgramCount(programId, runId, 3);

    // Signal the Workflow to continue
    elseForkOneActionDoneFile.createNewFile();
    elseForkAnotherActionDoneFile.createNewFile();
    elseForkThirdActionDoneFile.createNewFile();

    verifyProgramRuns(programId, "completed");

    List<RunRecord> workflowHistoryRuns = getProgramRuns(programId, "completed");

    Id.Program recordVerifierProgramId = Id.Program.from(TEST_NAMESPACE2, conditionalWorkflowApp, ProgramType.MAPREDUCE,
                                              "RecordVerifier");

    List<RunRecord> recordVerifierRuns = getProgramRuns(recordVerifierProgramId, "completed");

    Id.Program wordCountProgramId = Id.Program.from(TEST_NAMESPACE2, conditionalWorkflowApp, ProgramType.MAPREDUCE,
                                              "ClassicWordCount");

    List<RunRecord> wordCountRuns = getProgramRuns(wordCountProgramId, "completed");

    Assert.assertEquals(1, workflowHistoryRuns.size());
    Assert.assertEquals(1, recordVerifierRuns.size());
    Assert.assertEquals(0, wordCountRuns.size());

    // create input data in which number of good records are greater than the number of bad records
    runtimeArguments.put("inputPath", createConditionInput("AnotherConditionProgramInput", 10, 2));
    runtimeArguments.put("mapreduce.RecordVerifier.outputPath", new File(tmpFolder.newFolder(),
                                                                         "ConditionProgramOutput").getAbsolutePath());
    runtimeArguments.put("mapreduce.ClassicWordCount.outputPath", new File(tmpFolder.newFolder(),
                                                                         "ConditionProgramOutput").getAbsolutePath());

    setAndTestRuntimeArgs(programId, runtimeArguments);

    // Start the workflow
    startProgram(programId, 200);

    // Since the number of good records are greater than the number of bad records,
    // 'if' branch of the condition will get executed.
    // Wait till the execution of the fork on the if branch starts
    while (!(ifForkOneActionFile.exists() && ifForkAnotherActionFile.exists())) {
      TimeUnit.MILLISECONDS.sleep(50);
    }

    // Get running program run
    historyRuns = getProgramRuns(programId, "running");
    Assert.assertTrue(historyRuns.size() == 1);
    runId = historyRuns.get(0).getPid();

    // Since the fork on the if branch of the condition has 2 parallel branches
    // there should be 2 programs currently running
    verifyRunningProgramCount(programId, runId, 2);

    // Signal the Workflow to continue
    ifForkOneActionDoneFile.createNewFile();
    ifForkAnotherActionDoneFile.createNewFile();

    verifyProgramRuns(programId, "completed", 1);

    workflowHistoryRuns = getProgramRuns(programId, "completed");
    recordVerifierRuns = getProgramRuns(recordVerifierProgramId, "completed");
    wordCountRuns = getProgramRuns(wordCountProgramId, "completed");

    Assert.assertEquals(2, workflowHistoryRuns.size());
    Assert.assertEquals(2, recordVerifierRuns.size());
    Assert.assertEquals(1, wordCountRuns.size());
  }
}
