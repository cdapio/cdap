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

import co.cask.cdap.AppWithSchedule;
import co.cask.cdap.AppWithStreamSizeSchedule;
import co.cask.cdap.AppWithWorkflow;
import co.cask.cdap.ConcurrentWorkflowApp;
import co.cask.cdap.ConditionalWorkflowApp;
import co.cask.cdap.PauseResumeWorklowApp;
import co.cask.cdap.WorkflowAppWithErrorRuns;
import co.cask.cdap.WorkflowAppWithFork;
import co.cask.cdap.WorkflowAppWithLocalDatasets;
import co.cask.cdap.WorkflowAppWithScopedParameters;
import co.cask.cdap.WorkflowFailureInForkApp;
import co.cask.cdap.WorkflowTokenTestPutApp;
import co.cask.cdap.api.customaction.CustomActionSpecification;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.gateway.handlers.WorkflowHttpHandler;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.ScheduledRuntime;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.WorkflowTokenDetail;
import co.cask.cdap.proto.WorkflowTokenNodeDetail;
import co.cask.cdap.proto.codec.CustomActionSpecificationCodec;
import co.cask.cdap.proto.codec.ScheduleSpecificationCodec;
import co.cask.cdap.proto.codec.WorkflowActionSpecificationCodec;
import co.cask.cdap.proto.codec.WorkflowTokenDetailCodec;
import co.cask.cdap.proto.codec.WorkflowTokenNodeDetailCodec;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.test.XSlowTests;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Tests for {@link WorkflowHttpHandler}
 */
public class WorkflowHttpHandlerTest  extends AppFabricTestBase {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(ScheduleSpecification.class, new ScheduleSpecificationCodec())
    .registerTypeAdapter(WorkflowActionSpecification.class, new WorkflowActionSpecificationCodec())
    .registerTypeAdapter(CustomActionSpecification.class, new CustomActionSpecificationCodec())
    .registerTypeAdapter(WorkflowTokenDetail.class, new WorkflowTokenDetailCodec())
    .registerTypeAdapter(WorkflowTokenNodeDetail.class, new WorkflowTokenNodeDetailCodec())
    .create();

  private static final Type LIST_WORKFLOWACTIONNODE_TYPE = new TypeToken<List<WorkflowActionNode>>() { }.getType();
  private static final Type MAP_STRING_TO_WORKFLOWNODESTATEDETAIL_TYPE
    = new TypeToken<Map<String, WorkflowNodeStateDetail>>() { }.getType();
  private static final Type MAP_STRING_TO_DATASETSPECIFICATIONSUMMARY_TYPE
    = new TypeToken<Map<String, DatasetSpecificationSummary>>() { }.getType();


  private void verifyRunningProgramCount(final Id.Program program, final String runId, final int expected)
    throws Exception {
    Tasks.waitFor(expected, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return runningProgramCount(program, runId);
      }
    }, 10, TimeUnit.SECONDS);
  }

  private Integer runningProgramCount(Id.Program program, String runId) throws Exception {
    String path = String.format("apps/%s/workflows/%s/runs/%s/current", program.getApplicationId(), program.getId(),
                                runId);
    HttpResponse response = doGet(getVersionedAPIPath(path, program.getNamespaceId()));
    if (response.getStatusLine().getStatusCode() == 200) {
      String json = EntityUtils.toString(response.getEntity());
      List<WorkflowActionNode> output = GSON.fromJson(json, LIST_WORKFLOWACTIONNODE_TYPE);
      return output.size();
    }
    return null;
  }

  private void waitForStatusCode(final String path, final Id.Program program, final int expectedStatusCode)
    throws Exception {
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        HttpResponse response = doPost(getVersionedAPIPath(path, Constants.Gateway.API_VERSION_3_TOKEN,
                                                           program.getNamespaceId()));
        return expectedStatusCode == response.getStatusLine().getStatusCode();
      }
    }, 60, TimeUnit.SECONDS);
  }

  private void suspendWorkflow(Id.Program program, String runId, int expectedStatusCode) throws Exception {
    String path = String.format("apps/%s/workflows/%s/runs/%s/suspend", program.getApplicationId(), program.getId(),
                                runId);
    waitForStatusCode(path, program, expectedStatusCode);
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
    Map<String, String> argsRead = GSON.fromJson(responseEntity, new TypeToken<Map<String, String>>() {
    }.getType());

    Assert.assertEquals(args.size(), argsRead.size());
  }

  /**
   * Tries to resume a Workflow and expect the call completed with the status.
   */
  private void resumeWorkflow(Id.Program program, String runId, int expectedStatusCode) throws Exception {
    String path = String.format("apps/%s/workflows/%s/runs/%s/resume", program.getApplicationId(), program.getId(),
                                runId);

    waitForStatusCode(path, program, expectedStatusCode);
  }

  private HttpResponse getWorkflowCurrentStatus(Id.Program program, String runId) throws Exception {
    String currentUrl = String.format("apps/%s/workflows/%s/runs/%s/current", program.getApplicationId(),
                                      program.getId(), runId);
    String versionedUrl = getVersionedAPIPath(currentUrl, Constants.Gateway.API_VERSION_3_TOKEN,
                                              program.getNamespaceId());
    return doGet(versionedUrl);
  }

  private String createInput(String folderName) throws IOException {
    File inputDir = tmpFolder.newFolder(folderName);

    File inputFile = new File(inputDir.getPath() + "/words.txt");
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile.toPath(), Charsets.UTF_8)) {
      writer.write("this text has");
      writer.newLine();
      writer.write("two words text inside");
    }
    return inputDir.getAbsolutePath();
  }

  /**
   * Returns a list of {@link ScheduledRuntime}.
   *
   * @param program the program id
   * @param next if true, fetch the list of future run times. If false, fetch the list of past run times.
   */
  private List<ScheduledRuntime> getScheduledRunTime(Id.Program program, boolean next) throws Exception {
    String nextRunTimeUrl = String.format("apps/%s/workflows/%s/%sruntime", program.getApplicationId(), program.getId(),
                                          next ? "next" : "previous");
    String versionedUrl = getVersionedAPIPath(nextRunTimeUrl, Constants.Gateway.API_VERSION_3_TOKEN,
                                              program.getNamespaceId());
    HttpResponse response = doGet(versionedUrl);
    return readResponse(response, new TypeToken<List<ScheduledRuntime>>() { }.getType());
  }

  private String getLocalDatasetPath(ProgramId workflowId, String runId) {
    String path = String.format("apps/%s/workflows/%s/runs/%s/localdatasets", workflowId.getApplication(),
                                workflowId.getProgram(), runId);

    return getVersionedAPIPath(path, Constants.Gateway.API_VERSION_3_TOKEN, workflowId.getNamespace());
  }

  private Map<String, DatasetSpecificationSummary> getWorkflowLocalDatasets(ProgramId workflowId, String runId)
    throws Exception {
    HttpResponse response = doGet(getLocalDatasetPath(workflowId, runId));
    return readResponse(response, MAP_STRING_TO_DATASETSPECIFICATIONSUMMARY_TYPE);
  }

  private void deleteWorkflowLocalDatasets(ProgramId workflowId, String runId) throws Exception {
    doDelete(getLocalDatasetPath(workflowId, runId));
  }

  @Test
  public void testLocalDatasetDeletion() throws Exception {
    String keyValueTableType = "co.cask.cdap.api.dataset.lib.KeyValueTable";
    String filesetType = "co.cask.cdap.api.dataset.lib.FileSet";
    Map<String, String> keyValueTableProperties = ImmutableMap.of("foo", "bar");
    Map<String, String> filesetProperties = ImmutableMap.of("anotherFoo", "anotherBar");

    HttpResponse response = deploy(WorkflowAppWithLocalDatasets.class,
                                   Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    File waitFile = new File(tmpFolder.newFolder() + "/wait.file");
    File doneFile = new File(tmpFolder.newFolder() + "/done.file");

    ProgramId workflowId = new ProgramId(TEST_NAMESPACE2, WorkflowAppWithLocalDatasets.NAME, ProgramType.WORKFLOW,
                                         WorkflowAppWithLocalDatasets.WORKFLOW_NAME);

    startProgram(workflowId.toId(), ImmutableMap.of("wait.file", waitFile.getAbsolutePath(),
                                                    "done.file", doneFile.getAbsolutePath(),
                                                    "dataset.*.keep.local", "true"));

    while (!waitFile.exists()) {
      TimeUnit.MILLISECONDS.sleep(50);
    }

    String runId = getRunIdOfRunningProgram(workflowId.toId());

    doneFile.createNewFile();

    waitState(workflowId.toId(), ProgramStatus.STOPPED.name());

    Map<String, DatasetSpecificationSummary> localDatasetSummaries = getWorkflowLocalDatasets(workflowId, runId);
    Assert.assertEquals(2, localDatasetSummaries.size());
    DatasetSpecificationSummary keyValueTableSummary = new DatasetSpecificationSummary("MyTable." + runId,
                                                                                       keyValueTableType,
                                                                                       keyValueTableProperties);
    Assert.assertEquals(keyValueTableSummary, localDatasetSummaries.get("MyTable"));
    DatasetSpecificationSummary filesetSummary = new DatasetSpecificationSummary("MyFile." + runId, filesetType,
                                                                                 filesetProperties);
    Assert.assertEquals(filesetSummary, localDatasetSummaries.get("MyFile"));

    deleteWorkflowLocalDatasets(workflowId, runId);

    localDatasetSummaries = getWorkflowLocalDatasets(workflowId, runId);
    Assert.assertEquals(0, localDatasetSummaries.size());

    waitFile = new File(tmpFolder.newFolder() + "/wait.file");
    doneFile = new File(tmpFolder.newFolder() + "/done.file");

    startProgram(workflowId.toId(), ImmutableMap.of("wait.file", waitFile.getAbsolutePath(),
                                                    "done.file", doneFile.getAbsolutePath(),
                                                    "dataset.MyTable.keep.local", "true"));

    while (!waitFile.exists()) {
      TimeUnit.MILLISECONDS.sleep(50);
    }

    runId = getRunIdOfRunningProgram(workflowId.toId());

    doneFile.createNewFile();

    waitState(workflowId.toId(), ProgramStatus.STOPPED.name());
    localDatasetSummaries = getWorkflowLocalDatasets(workflowId, runId);
    Assert.assertEquals(1, localDatasetSummaries.size());
    keyValueTableSummary = new DatasetSpecificationSummary("MyTable." + runId, keyValueTableType,
                                                           keyValueTableProperties);

    Assert.assertEquals(keyValueTableSummary, localDatasetSummaries.get("MyTable"));
    deleteWorkflowLocalDatasets(workflowId, runId);

    localDatasetSummaries = getWorkflowLocalDatasets(workflowId, runId);
    Assert.assertEquals(0, localDatasetSummaries.size());
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
    waitState(programId, ProgramStatus.RUNNING.name());

    // Get runid for the running Workflow
    String runId = getRunIdOfRunningProgram(programId);

    while (!firstSimpleActionFile.exists()) {
      TimeUnit.MILLISECONDS.sleep(50);
    }

    // Only one Workflow node should be running
    verifyRunningProgramCount(programId, runId, 1);

    // Suspend the Workflow
    suspendWorkflow(programId, runId, 200);

    // Workflow status hould be SUSPENDED
    waitState(programId, ProgramStatus.STOPPED.name());

    // Meta store information for this Workflow should reflect suspended run
    verifyProgramRuns(programId, "suspended");

    // Suspending the already suspended Workflow should give CONFLICT
    suspendWorkflow(programId, runId, 409);

    // Signal the FirstSimpleAction in the Workflow to continue
    Assert.assertTrue(firstSimpleActionDoneFile.createNewFile());

    // Even if the Workflow is suspended, currently executing action will complete and currently running nodes
    // should be zero
    verifyRunningProgramCount(programId, runId, 0);

    // Verify that Workflow is still suspended
    verifyProgramRuns(programId, "suspended");

    // Resume the execution of the Workflow
    resumeWorkflow(programId, runId, 200);

    // Workflow should be running
    waitState(programId, ProgramStatus.RUNNING.name());

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
    waitState(programId, ProgramStatus.STOPPED.name());

    // Store should reflect the suspended status of the Workflow
    verifyProgramRuns(programId, "suspended");

    // Allow currently executing actions to complete
    Assert.assertTrue(forkedSimpleActionDoneFile.createNewFile());
    Assert.assertTrue(anotherForkedSimpleActionDoneFile.createNewFile());

    // Workflow should have zero actions running
    verifyRunningProgramCount(programId, runId, 0);

    verifyProgramRuns(programId, "suspended");

    Assert.assertTrue(!lastSimpleActionFile.exists());

    resumeWorkflow(programId, runId, 200);

    waitState(programId, ProgramStatus.RUNNING.name());

    while (!lastSimpleActionFile.exists()) {
      TimeUnit.SECONDS.sleep(1);
    }

    verifyRunningProgramCount(programId, runId, 1);

    Assert.assertTrue(lastSimpleActionDoneFile.createNewFile());

    verifyProgramRuns(programId, "completed");

    waitState(programId, ProgramStatus.STOPPED.name());

    suspendWorkflow(programId, runId, 404);

    resumeWorkflow(programId, runId, 404);

  }

  @Category(XSlowTests.class)
  @Test
  public void testMultipleWorkflowInstances() throws Exception {
    String appWithConcurrentWorkflow = ConcurrentWorkflowApp.class.getSimpleName();

    // Files used to synchronize between this test and workflow execution
    File tempDir = tmpFolder.newFolder(appWithConcurrentWorkflow);
    File run1File = new File(tempDir, "concurrentRun1.file");
    File run2File = new File(tempDir, "concurrentRun2.file");
    File run1DoneFile = new File(tempDir, "concurrentRun1.done");
    File run2DoneFile = new File(tempDir, "concurrentRun2.done");

    // create app in default namespace so that v2 and v3 api can be tested in the same test
    String defaultNamespace = Id.Namespace.DEFAULT.getId();
    HttpResponse response = deploy(ConcurrentWorkflowApp.class, Constants.Gateway.API_VERSION_3_TOKEN,
                                   defaultNamespace);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Id.Program programId = Id.Program.from(Id.Namespace.DEFAULT, appWithConcurrentWorkflow, ProgramType.WORKFLOW,
                                           ConcurrentWorkflowApp.ConcurrentWorkflow.class.getSimpleName());

    // start run 1
    startProgram(programId, ImmutableMap.of(ConcurrentWorkflowApp.FILE_TO_CREATE_ARG, run1File.getAbsolutePath(),
                                            ConcurrentWorkflowApp.DONE_FILE_ARG, run1DoneFile.getAbsolutePath()),
                 200);
    // start run 2
    startProgram(programId, ImmutableMap.of(ConcurrentWorkflowApp.FILE_TO_CREATE_ARG, run2File.getAbsolutePath(),
                                            ConcurrentWorkflowApp.DONE_FILE_ARG, run2DoneFile.getAbsolutePath()),
                 200);

    while (!(run1File.exists() && run2File.exists())) {
      TimeUnit.MILLISECONDS.sleep(50);
    }

    verifyMultipleConcurrentRuns(programId);

    Assert.assertTrue(run1DoneFile.createNewFile());
    Assert.assertTrue(run2DoneFile.createNewFile());

    waitState(programId, ProgramStatus.STOPPED.name());
    // delete the application
    deleteApp(programId.getApplication(), 200, 60, TimeUnit.SECONDS);
  }

  private void verifyMultipleConcurrentRuns(Id.Program workflowId) throws Exception {
    verifyProgramRuns(workflowId, ProgramRunStatus.RUNNING.name(), 1);
    List<RunRecord> historyRuns = getProgramRuns(workflowId, "running");
    Assert.assertEquals(2, historyRuns.size());

    HttpResponse response = getWorkflowCurrentStatus(workflowId, historyRuns.get(0).getPid());
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String json = EntityUtils.toString(response.getEntity());
    List<WorkflowActionNode> nodes = GSON.fromJson(json, LIST_WORKFLOWACTIONNODE_TYPE);
    Assert.assertEquals(1, nodes.size());
    Assert.assertEquals(ConcurrentWorkflowApp.SimpleAction.class.getSimpleName(),
                        nodes.get(0).getProgram().getProgramName());

    response = getWorkflowCurrentStatus(workflowId, historyRuns.get(1).getPid());
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    json = EntityUtils.toString(response.getEntity());
    nodes = GSON.fromJson(json, LIST_WORKFLOWACTIONNODE_TYPE);
    Assert.assertEquals(1, nodes.size());
    Assert.assertEquals(ConcurrentWorkflowApp.SimpleAction.class.getSimpleName(),
                        nodes.get(0).getProgram().getProgramName());
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
    }, 180, TimeUnit.SECONDS);
  }

  @Test
  public void testWorkflowForkApp() throws Exception {
    File directory = tmpFolder.newFolder();
    Map<String, String> runtimeArgs = new HashMap<>();

    // Files used to synchronize between this test and workflow execution
    File firstFile = new File(directory, "first.file");
    File firstDoneFile = new File(directory, "first.done");
    runtimeArgs.put("first.file", firstFile.getAbsolutePath());
    runtimeArgs.put("first.donefile", firstDoneFile.getAbsolutePath());

    File branch1File = new File(directory, "branch1.file");
    File branch1DoneFile = new File(directory, "branch1.done");
    runtimeArgs.put("branch1.file", branch1File.getAbsolutePath());
    runtimeArgs.put("branch1.donefile", branch1DoneFile.getAbsolutePath());

    File branch2File = new File(directory, "branch2.file");
    File branch2DoneFile = new File(directory, "branch2.done");
    runtimeArgs.put("branch2.file", branch2File.getAbsolutePath());
    runtimeArgs.put("branch2.donefile", branch2DoneFile.getAbsolutePath());

    HttpResponse response = deploy(WorkflowAppWithFork.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Id.Program programId = Id.Program.from(
      TEST_NAMESPACE2, WorkflowAppWithFork.class.getSimpleName(), ProgramType.WORKFLOW,
      WorkflowAppWithFork.WorkflowWithFork.class.getSimpleName());

    setAndTestRuntimeArgs(programId, runtimeArgs);

    // Start a Workflow
    startProgram(programId);

    // Workflow should be running
    waitState(programId, ProgramStatus.RUNNING.name());

    // Get the runId for the currently running Workflow
    String runId = getRunIdOfRunningProgram(programId);

    // Wait till first action in the Workflow starts executing
    verifyFileExists(Lists.newArrayList(firstFile));

    verifyRunningProgramCount(programId, runId, 1);

    // Stop the Workflow
    stopProgram(programId);

    // Workflow run record should be marked 'killed'
    verifyProgramRuns(programId, "killed");

    // Delete the asset created in the previous run
    Assert.assertTrue(firstFile.delete());

    // Start the Workflow again
    startProgram(programId);

    // Workflow should be running
    waitState(programId, ProgramStatus.RUNNING.name());

    // Get the runId for the currently running Workflow
    String newRunId = getRunIdOfRunningProgram(programId);
    Assert.assertTrue(
      String.format("Expected a new runId to be generated after starting the workflow for the second time, but " +
                      "found old runId '%s' = new runId '%s'", runId, newRunId), !runId.equals(newRunId));

    // Store the new RunId
    runId = newRunId;

    // Wait till first action in the Workflow starts executing
    verifyFileExists(Lists.newArrayList(firstFile));

    verifyRunningProgramCount(programId, runId, 1);

    // Signal the first action to continue
    Assert.assertTrue(firstDoneFile.createNewFile());

    // Wait till fork in the Workflow starts executing
    verifyFileExists(Lists.newArrayList(branch1File, branch2File));

    // Two actions should be running in Workflow as a part of the fork
    verifyRunningProgramCount(programId, runId, 2);

    // Stop the program while in fork
    stopProgram(programId, 200);

    // Wait till the program stop
    waitState(programId, ProgramStatus.STOPPED.name());

    // Current endpoint would return 404
    response = getWorkflowCurrentStatus(programId, runId);
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    // Now there should be 2 RunRecord with status killed
    verifyProgramRuns(programId, "killed", 1);

    // Delete the assets generated in the previous run
    Assert.assertTrue(firstFile.delete());
    Assert.assertTrue(firstDoneFile.delete());
    Assert.assertTrue(branch1File.delete());
    Assert.assertTrue(branch2File.delete());

    // Restart the run again
    startProgram(programId);

    // Wait till the Workflow is running
    waitState(programId, ProgramStatus.RUNNING.name());

    // Store the new RunRecord for the currently running run
    runId = getRunIdOfRunningProgram(programId);

    // Wait till first action in the Workflow starts executing
    verifyFileExists(Lists.newArrayList(firstFile));

    verifyRunningProgramCount(programId, runId, 1);

    // Signal the first action to continue
    Assert.assertTrue(firstDoneFile.createNewFile());

    // Wait till fork in the Workflow starts executing
    verifyFileExists(Lists.newArrayList(branch1File, branch2File));

    // Two actions should be running in Workflow as a part of the fork
    verifyRunningProgramCount(programId, runId, 2);

    // Signal the Workflow that execution can be continued
    Assert.assertTrue(branch1DoneFile.createNewFile());
    Assert.assertTrue(branch2DoneFile.createNewFile());

    // Workflow should now have one completed run
    verifyProgramRuns(programId, "completed");
  }

  private String getRunIdOfRunningProgram(final Id.Program programId) throws Exception {
    Tasks.waitFor(1, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return getProgramRuns(programId, "running").size();
      }
    }, 5, TimeUnit.SECONDS);
    List<RunRecord> historyRuns = getProgramRuns(programId, "running");
    Assert.assertEquals(1, historyRuns.size());
    RunRecord record = historyRuns.get(0);
    return record.getPid();
  }

  private Map<String, WorkflowNodeStateDetail> getWorkflowNodeStates(ProgramId workflowId, String runId)
    throws Exception {
    String path = String.format("apps/%s/workflows/%s/runs/%s/nodes/state", workflowId.getApplication(),
                                workflowId.getProgram(), runId);

    path = getVersionedAPIPath(path, Constants.Gateway.API_VERSION_3_TOKEN, workflowId.getNamespace());

    HttpResponse response = doGet(path);
    return readResponse(response, MAP_STRING_TO_WORKFLOWNODESTATEDETAIL_TYPE);
  }

  @Category(XSlowTests.class)
  @Test
  public void testWorkflowScopedArguments() throws Exception {
    String workflowRunIdProperty = "workflowrunid";
    HttpResponse response = deploy(WorkflowAppWithScopedParameters.class, Constants.Gateway.API_VERSION_3_TOKEN,
                                   TEST_NAMESPACE2);

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    ProgramId programId = Ids.namespace(TEST_NAMESPACE2).app(WorkflowAppWithScopedParameters.APP_NAME)
      .workflow(WorkflowAppWithScopedParameters.ONE_WORKFLOW);

    Map<String, String> runtimeArguments = Maps.newHashMap();

    runtimeArguments.put("debug", "true");
    runtimeArguments.put("mapreduce.*.debug", "false");
    runtimeArguments.put("mapreduce.OneMR.debug", "true");

    runtimeArguments.put("input.path", createInput("ProgramInput"));
    runtimeArguments.put("mapreduce.OneMR.input.path", createInput("OneMRInput"));
    runtimeArguments.put("mapreduce.OneMR.logical.start.time", "1234567890000");
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

    setAndTestRuntimeArgs(programId.toId(), runtimeArguments);

    // Start the workflow
    startProgram(programId.toId());
    waitState(programId.toId(), ProgramStatus.RUNNING.name());

    // Wait until we have a run record
    verifyProgramRuns(programId.toId(), "running");
    List<RunRecord> workflowHistoryRuns = getProgramRuns(programId.toId(), "running");
    String workflowRunId = workflowHistoryRuns.get(0).getPid();

    Id.Program mr1ProgramId = Id.Program.from(TEST_NAMESPACE2, WorkflowAppWithScopedParameters.APP_NAME,
                                              ProgramType.MAPREDUCE, WorkflowAppWithScopedParameters.ONE_MR);
    waitState(mr1ProgramId, ProgramStatus.RUNNING.name());
    List<RunRecord> oneMRHistoryRuns = getProgramRuns(mr1ProgramId, "running");

    String expectedMessage = String.format("Cannot stop the program '%s' started by the Workflow run '%s'. " +
                                             "Please stop the Workflow.",
                                           new Id.Run(mr1ProgramId, oneMRHistoryRuns.get(0).getPid()), workflowRunId);
    stopProgram(mr1ProgramId, oneMRHistoryRuns.get(0).getPid(), 400, expectedMessage);

    verifyProgramRuns(programId.toId(), "completed");

    workflowHistoryRuns = getProgramRuns(programId.toId(), "completed");

    oneMRHistoryRuns = getProgramRuns(mr1ProgramId, "completed");

    Id.Program mr2ProgramId = Id.Program.from(TEST_NAMESPACE2, WorkflowAppWithScopedParameters.APP_NAME,
                                              ProgramType.MAPREDUCE, WorkflowAppWithScopedParameters.ANOTHER_MR);

    List<RunRecord> anotherMRHistoryRuns = getProgramRuns(mr2ProgramId, "completed");

    Id.Program spark1ProgramId = Id.Program.from(TEST_NAMESPACE2, WorkflowAppWithScopedParameters.APP_NAME,
                                                 ProgramType.SPARK, WorkflowAppWithScopedParameters.ONE_SPARK);

    List<RunRecord> oneSparkHistoryRuns = getProgramRuns(spark1ProgramId, "completed");

    Id.Program spark2ProgramId = Id.Program.from(TEST_NAMESPACE2, WorkflowAppWithScopedParameters.APP_NAME,
                                                 ProgramType.SPARK, WorkflowAppWithScopedParameters.ANOTHER_SPARK);

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

    Assert.assertNotNull(oneMRRunRecordProperties.get(workflowRunIdProperty));
    Assert.assertEquals(workflowHistoryRuns.get(0).getPid(), oneMRRunRecordProperties.get(workflowRunIdProperty));

    Assert.assertNotNull(anotherMRRunRecordProperties.get(workflowRunIdProperty));
    Assert.assertEquals(workflowHistoryRuns.get(0).getPid(), anotherMRRunRecordProperties.get(workflowRunIdProperty));

    Assert.assertNotNull(oneSparkRunRecordProperties.get(workflowRunIdProperty));
    Assert.assertEquals(workflowHistoryRuns.get(0).getPid(), oneSparkRunRecordProperties.get(workflowRunIdProperty));

    Assert.assertNotNull(anotherSparkRunRecordProperties.get(workflowRunIdProperty));
    Assert.assertEquals(workflowHistoryRuns.get(0).getPid(),
                        anotherSparkRunRecordProperties.get(workflowRunIdProperty));

    Assert.assertEquals(workflowRunRecordProperties.get(WorkflowAppWithScopedParameters.ONE_MR),
                        oneMRHistoryRuns.get(0).getPid());
    Assert.assertEquals(workflowRunRecordProperties.get(WorkflowAppWithScopedParameters.ONE_SPARK),
                        oneSparkHistoryRuns.get(0).getPid());
    Assert.assertEquals(workflowRunRecordProperties.get(WorkflowAppWithScopedParameters.ANOTHER_MR),
                        anotherMRHistoryRuns.get(0).getPid());
    Assert.assertEquals(workflowRunRecordProperties.get(WorkflowAppWithScopedParameters.ANOTHER_SPARK),
                        anotherSparkHistoryRuns.get(0).getPid());


    // Get Workflow node states
    Map<String, WorkflowNodeStateDetail> nodeStates = getWorkflowNodeStates(programId,
                                                                            workflowHistoryRuns.get(0).getPid());

    Assert.assertNotNull(nodeStates);
    Assert.assertEquals(5, nodeStates.size());
    WorkflowNodeStateDetail mrNodeState = nodeStates.get(WorkflowAppWithScopedParameters.ONE_MR);
    Assert.assertNotNull(mrNodeState);
    Assert.assertEquals(WorkflowAppWithScopedParameters.ONE_MR, mrNodeState.getNodeId());
    Assert.assertEquals(oneMRHistoryRuns.get(0).getPid(), mrNodeState.getRunId());

    mrNodeState = nodeStates.get(WorkflowAppWithScopedParameters.ANOTHER_MR);
    Assert.assertNotNull(mrNodeState);
    Assert.assertEquals(WorkflowAppWithScopedParameters.ANOTHER_MR, mrNodeState.getNodeId());
    Assert.assertEquals(anotherMRHistoryRuns.get(0).getPid(), mrNodeState.getRunId());

    WorkflowNodeStateDetail sparkNodeState = nodeStates.get(WorkflowAppWithScopedParameters.ONE_SPARK);
    Assert.assertNotNull(sparkNodeState);
    Assert.assertEquals(WorkflowAppWithScopedParameters.ONE_SPARK, sparkNodeState.getNodeId());
    Assert.assertEquals(oneSparkHistoryRuns.get(0).getPid(), sparkNodeState.getRunId());

    sparkNodeState = nodeStates.get(WorkflowAppWithScopedParameters.ANOTHER_SPARK);
    Assert.assertNotNull(sparkNodeState);
    Assert.assertEquals(WorkflowAppWithScopedParameters.ANOTHER_SPARK, sparkNodeState.getNodeId());
    Assert.assertEquals(anotherSparkHistoryRuns.get(0).getPid(), sparkNodeState.getRunId());

    WorkflowNodeStateDetail oneActionNodeState = nodeStates.get(WorkflowAppWithScopedParameters.ONE_ACTION);
    Assert.assertNotNull(oneActionNodeState);
    Assert.assertEquals(WorkflowAppWithScopedParameters.ONE_ACTION, oneActionNodeState.getNodeId());
  }

  @Ignore
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

    Map<String, String> runtimeArguments = ImmutableMap.of("someKey", "someWorkflowValue",
                                                           "workflowKey", "workflowValue");

    setAndTestRuntimeArgs(programId, runtimeArguments);

    // get schedules
    List<ScheduleSpecification> schedules = getSchedules(TEST_NAMESPACE2, appName, workflowName);
    Assert.assertEquals(1, schedules.size());
    String scheduleName = schedules.get(0).getSchedule().getName();
    Assert.assertFalse(scheduleName.isEmpty());

    // TODO [CDAP-2327] Sagar Investigate why following check fails sometimes. Mostly test case issue.
    // List<ScheduledRuntime> previousRuntimes = getScheduledRunTime(programId, scheduleName, "previousruntime");
    // Assert.assertTrue(previousRuntimes.size() == 0);

    long current = System.currentTimeMillis();

    // Resume the schedule
    Assert.assertEquals(200, resumeSchedule(TEST_NAMESPACE2, appName, sampleSchedule));
    // Check schedule status
    assertSchedule(programId, scheduleName, true, 30, TimeUnit.SECONDS);

    List<ScheduledRuntime> runtimes = getScheduledRunTime(programId, true);
    String id = runtimes.get(0).getId();
    Assert.assertTrue(String.format("Expected schedule id '%s' to contain schedule name '%s'", id, scheduleName),
                      id.contains(scheduleName));
    Long nextRunTime = runtimes.get(0).getTime();
    Assert.assertTrue(String.format("Expected nextRuntime '%s' to be greater than current runtime '%s'",
                                    nextRunTime, current),
                      nextRunTime > current);

    // Verify that at least one program is completed
    verifyProgramRuns(programId, "completed");

    // Suspend the schedule
    Assert.assertEquals(200, suspendSchedule(TEST_NAMESPACE2, appName, scheduleName));
    // check paused state
    assertSchedule(programId, scheduleName, false, 30, TimeUnit.SECONDS);

    // check that there were at least 1 previous runs
    List<ScheduledRuntime> previousRuntimes = getScheduledRunTime(programId, false);
    int numRuns = previousRuntimes.size();
    Assert.assertTrue(String.format("After sleeping for two seconds, the schedule should have at least triggered " +
                                      "once, but found %s previous runs", numRuns), numRuns >= 1);

    // Verify no program running
    verifyNoRunWithStatus(programId, "running");

    // get number of completed runs after schedule is suspended
    int workflowRuns = getProgramRuns(programId, "completed").size();

    // verify that resuming the suspended schedule again has expected behavior (spawns new runs)
    Assert.assertEquals(200, resumeSchedule(TEST_NAMESPACE2, appName, scheduleName));
    //check scheduled state
    assertSchedule(programId, scheduleName, true, 30, TimeUnit.SECONDS);

    // Verify that the program ran after the schedule was resumed
    verifyProgramRuns(programId, "completed", workflowRuns);

    // Suspend the schedule
    Assert.assertEquals(200, suspendSchedule(TEST_NAMESPACE2, appName, scheduleName));
    //check paused state
    assertSchedule(programId, scheduleName, false, 30, TimeUnit.SECONDS);

    //Check status of a non existing schedule
    try {
      assertSchedule(programId, "invalid", true, 2, TimeUnit.SECONDS);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }

    //Schedule operations using invalid namespace
    try {
      assertSchedule(Id.Program.from(TEST_NAMESPACE1, appName, ProgramType.WORKFLOW, workflowName),
                     scheduleName, true, 2, TimeUnit.SECONDS);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }
    Assert.assertEquals(404, suspendSchedule(TEST_NAMESPACE1, appName, scheduleName));
    Assert.assertEquals(404, resumeSchedule(TEST_NAMESPACE1, appName, scheduleName));

    verifyNoRunWithStatus(programId, "running");
    deleteApp(Id.Application.from(TEST_NAMESPACE2, AppWithSchedule.class.getSimpleName()), 200);
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

    Id.Program programId = Id.Program.from(TEST_NAMESPACE2, appName, ProgramType.WORKFLOW, workflowName);

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

    // Only schedule 1 should get executed
    verifyProgramRuns(programId, "completed");

    //Check schedule status
    assertSchedule(programId, scheduleName1, true, 30, TimeUnit.SECONDS);
    assertSchedule(programId, scheduleName2, true, 30, TimeUnit.SECONDS);

    Assert.assertEquals(200, suspendSchedule(TEST_NAMESPACE2, appName, scheduleName1));
    Assert.assertEquals(200, suspendSchedule(TEST_NAMESPACE2, appName, scheduleName2));
    //check paused state
    assertSchedule(programId, scheduleName1, false, 30, TimeUnit.SECONDS);
    assertSchedule(programId, scheduleName2, false, 30, TimeUnit.SECONDS);

    int workflowRuns = getProgramRuns(programId, "completed").size();
    // Should still be one
    Assert.assertEquals(1, workflowRuns);

    // Sleep for some time and verify there are no more scheduled jobs after the suspend.
    for (int i = 0; i < 12; ++i) {
      response = doPost(String.format("/v3/namespaces/%s/streams/%s", TEST_NAMESPACE2, streamName), longString);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    }
    TimeUnit.SECONDS.sleep(5);

    int workflowRunsAfterSuspend = getProgramRuns(programId, "completed").size();
    Assert.assertEquals(workflowRuns, workflowRunsAfterSuspend);

    Assert.assertEquals(200, resumeSchedule(TEST_NAMESPACE2, appName, scheduleName1));

    assertRunHistory(programId, "completed", workflowRunsAfterSuspend, 60, TimeUnit.SECONDS);

    //check scheduled state
    assertSchedule(programId, scheduleName1, true, 30, TimeUnit.SECONDS);

    //Check status of a non existing schedule
    try {
      assertSchedule(programId, "invalid", true, 2, TimeUnit.SECONDS);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }

    Assert.assertEquals(200, suspendSchedule(TEST_NAMESPACE2, appName, scheduleName1));

    //check paused state
    assertSchedule(programId, scheduleName1, false, 30, TimeUnit.SECONDS);

    //Schedule operations using invalid namespace
    try {
      assertSchedule(Id.Program.from(TEST_NAMESPACE1, appName, ProgramType.WORKFLOW, workflowName),
                     scheduleName1, true, 2, TimeUnit.SECONDS);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }
    Assert.assertEquals(404, suspendSchedule(TEST_NAMESPACE1, appName, scheduleName1));
    Assert.assertEquals(404, resumeSchedule(TEST_NAMESPACE1, appName, scheduleName1));

    TimeUnit.SECONDS.sleep(2); //wait till any running jobs just before suspend call completes.
  }

  @Test
  public void testWorkflowRuns() throws Exception {
    String appName = "WorkflowAppWithErrorRuns";
    String workflowName = "WorkflowWithErrorRuns";

    HttpResponse response = deploy(WorkflowAppWithErrorRuns.class, Constants.Gateway.API_VERSION_3_TOKEN,
                                   TEST_NAMESPACE2);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Id.Program programId = Id.Program.from(TEST_NAMESPACE2, appName, ProgramType.WORKFLOW, workflowName);

    // Test the "KILLED" state of the Workflow.
    File instance1File = new File(tmpFolder.newFolder() + "/instance1.file");
    File instance2File = new File(tmpFolder.newFolder() + "/instance2.file");
    File doneFile = new File(tmpFolder.newFolder() + "/done.file");

    // Start the first Workflow run.
    Map<String, String> propertyMap = ImmutableMap.of("simple.action.file", instance1File.getAbsolutePath(),
                                                      "simple.action.donefile", doneFile.getAbsolutePath());
    startProgram(programId, propertyMap);

    // Start another Workflow run.
    propertyMap = ImmutableMap.of("simple.action.file", instance2File.getAbsolutePath(),
                                  "simple.action.donefile", doneFile.getAbsolutePath());
    startProgram(programId, propertyMap);

    // Wait till the execution of actions in both Workflow runs is started.
    while (!(instance1File.exists() && instance2File.exists())) {
      TimeUnit.MILLISECONDS.sleep(50);
    }

    // Verify that there are two runs of the Workflow currently running.
    List<RunRecord> historyRuns = getProgramRuns(programId, "running");
    Assert.assertEquals(2, historyRuns.size());

    // Stop both Workflow runs.
    String runId = historyRuns.get(0).getPid();
    stopProgram(programId, runId, 200);
    runId = historyRuns.get(1).getPid();
    stopProgram(programId, runId, 200);

    // Verify both runs should be marked "KILLED".
    verifyProgramRuns(programId, "killed", 1);

    // Test the "COMPLETE" state of the Workflow.
    File instanceFile = new File(tmpFolder.newFolder() + "/instance.file");
    propertyMap = ImmutableMap.of("simple.action.file", instanceFile.getAbsolutePath(),
                                  "simple.action.donefile", doneFile.getAbsolutePath());
    startProgram(programId, propertyMap);
    while (!instanceFile.exists()) {
      TimeUnit.MILLISECONDS.sleep(50);
    }
    // Verify that currently only one run of the Workflow should be running.
    historyRuns = getProgramRuns(programId, "running");
    Assert.assertEquals(1, historyRuns.size());

    Assert.assertTrue(doneFile.createNewFile());

    // Verify that Workflow should move to "COMPLETED" state.
    verifyProgramRuns(programId, "completed");

    // Test the "FAILED" state of the program.
    propertyMap = ImmutableMap.of("ThrowError", "true");
    startProgram(programId, propertyMap);

    // Verify that the Workflow should be marked as "FAILED".
    verifyProgramRuns(programId, "failed");
  }

  private String createConditionInput(String folderName, int numGoodRecords, int numBadRecords) throws IOException {
    File inputDir = tmpFolder.newFolder(folderName);

    File inputFile = new File(inputDir.getPath() + "/data.txt");

    try (BufferedWriter writer = Files.newBufferedWriter(inputFile.toPath(), Charsets.UTF_8)) {
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
    startProgram(programId);

    // Since the number of good records are lesser than the number of bad records,
    // 'else' branch of the condition will get executed.
    // Wait till the execution of the fork on the else branch starts
    while (!(elseForkOneActionFile.exists() &&
             elseForkAnotherActionFile.exists() &&
             elseForkThirdActionFile.exists())) {
      TimeUnit.MILLISECONDS.sleep(50);
    }

    // Get running program run
    String runId = getRunIdOfRunningProgram(programId);

    // Since the fork on the else branch of condition has 3 parallel branches
    // there should be 3 programs currently running
    verifyRunningProgramCount(programId, runId, 3);

    // Signal the Workflow to continue
    Assert.assertTrue(elseForkOneActionDoneFile.createNewFile());
    Assert.assertTrue(elseForkAnotherActionDoneFile.createNewFile());
    Assert.assertTrue(elseForkThirdActionDoneFile.createNewFile());

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
    startProgram(programId);

    // Since the number of good records are greater than the number of bad records,
    // 'if' branch of the condition will get executed.
    // Wait till the execution of the fork on the if branch starts
    while (!(ifForkOneActionFile.exists() && ifForkAnotherActionFile.exists())) {
      TimeUnit.MILLISECONDS.sleep(50);
    }

    // Get running program run
    runId = getRunIdOfRunningProgram(programId);

    // Since the fork on the if branch of the condition has 2 parallel branches
    // there should be 2 programs currently running
    verifyRunningProgramCount(programId, runId, 2);

    // Signal the Workflow to continue
    Assert.assertTrue(ifForkOneActionDoneFile.createNewFile());
    Assert.assertTrue(ifForkAnotherActionDoneFile.createNewFile());

    verifyProgramRuns(programId, "completed", 1);

    workflowHistoryRuns = getProgramRuns(programId, "completed");
    recordVerifierRuns = getProgramRuns(recordVerifierProgramId, "completed");
    wordCountRuns = getProgramRuns(wordCountProgramId, "completed");

    Assert.assertEquals(2, workflowHistoryRuns.size());
    Assert.assertEquals(2, recordVerifierRuns.size());
    Assert.assertEquals(1, wordCountRuns.size());
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testWorkflowToken() throws Exception {
    Assert.assertEquals(200, deploy(AppWithWorkflow.class).getStatusLine().getStatusCode());
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, AppWithWorkflow.NAME);
    final Id.Workflow workflowId = Id.Workflow.from(appId, AppWithWorkflow.SampleWorkflow.NAME);
    String outputPath = new File(tmpFolder.newFolder(), "output").getAbsolutePath();
    startProgram(workflowId, ImmutableMap.of("inputPath", createInput("input"),
                                             "outputPath", outputPath));

    Tasks.waitFor(1, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return getProgramRuns(workflowId, ProgramRunStatus.COMPLETED.name()).size();
      }
    }, 60, TimeUnit.SECONDS);
    List<RunRecord> programRuns = getProgramRuns(workflowId, ProgramRunStatus.COMPLETED.name());
    Assert.assertEquals(1, programRuns.size());
    RunRecord runRecord = programRuns.get(0);
    String pid = runRecord.getPid();
    // Verify entire worfklow token
    WorkflowTokenDetail workflowTokenDetail = getWorkflowToken(workflowId, pid, null, null);
    List<WorkflowTokenDetail.NodeValueDetail> nodeValueDetails =
      workflowTokenDetail.getTokenData().get(AppWithWorkflow.DummyAction.TOKEN_KEY);
    Assert.assertEquals(2, nodeValueDetails.size());
    Assert.assertEquals(AppWithWorkflow.SampleWorkflow.FIRST_ACTION, nodeValueDetails.get(0).getNode());
    Assert.assertEquals(AppWithWorkflow.SampleWorkflow.SECOND_ACTION, nodeValueDetails.get(1).getNode());
    Assert.assertEquals(AppWithWorkflow.DummyAction.TOKEN_VALUE, nodeValueDetails.get(0).getValue());
    Assert.assertEquals(AppWithWorkflow.DummyAction.TOKEN_VALUE, nodeValueDetails.get(1).getValue());
    // Verify entire workflow token by passing in the scope and key in the request
    workflowTokenDetail = getWorkflowToken(workflowId, pid, WorkflowToken.Scope.USER,
                                           AppWithWorkflow.DummyAction.TOKEN_KEY);
    nodeValueDetails = workflowTokenDetail.getTokenData().get(AppWithWorkflow.DummyAction.TOKEN_KEY);
    Assert.assertEquals(2, nodeValueDetails.size());
    Assert.assertEquals(AppWithWorkflow.SampleWorkflow.FIRST_ACTION, nodeValueDetails.get(0).getNode());
    Assert.assertEquals(AppWithWorkflow.SampleWorkflow.SECOND_ACTION, nodeValueDetails.get(1).getNode());
    Assert.assertEquals(AppWithWorkflow.DummyAction.TOKEN_VALUE, nodeValueDetails.get(0).getValue());
    Assert.assertEquals(AppWithWorkflow.DummyAction.TOKEN_VALUE, nodeValueDetails.get(1).getValue());

    // Get workflow level tokens
    WorkflowTokenNodeDetail nodeDetail = getWorkflowToken(workflowId, pid, AppWithWorkflow.SampleWorkflow.NAME,
                                                          WorkflowToken.Scope.USER, null);
    Map<String, String> tokenData = nodeDetail.getTokenDataAtNode();
    Assert.assertEquals(2, tokenData.size());
    Assert.assertEquals(AppWithWorkflow.SampleWorkflow.INITIALIZE_TOKEN_VALUE,
                        tokenData.get(AppWithWorkflow.SampleWorkflow.INITIALIZE_TOKEN_KEY));
    Assert.assertEquals(AppWithWorkflow.SampleWorkflow.DESTROY_TOKEN_SUCCESS_VALUE,
                        tokenData.get(AppWithWorkflow.SampleWorkflow.DESTROY_TOKEN_KEY));

    // Verify workflow token at a given node
    WorkflowTokenNodeDetail tokenAtNode = getWorkflowToken(workflowId, pid,
                                                           AppWithWorkflow.SampleWorkflow.FIRST_ACTION, null, null);
    Map<String, String> tokenDataAtNode = tokenAtNode.getTokenDataAtNode();
    Assert.assertEquals(1, tokenDataAtNode.size());
    Assert.assertEquals(AppWithWorkflow.DummyAction.TOKEN_VALUE,
                        tokenDataAtNode.get(AppWithWorkflow.DummyAction.TOKEN_KEY));
    // Verify workflow token at a given node by passing in a scope and a key
    tokenAtNode = getWorkflowToken(workflowId, pid, AppWithWorkflow.SampleWorkflow.FIRST_ACTION,
                                   WorkflowToken.Scope.USER, AppWithWorkflow.DummyAction.TOKEN_KEY);
    tokenDataAtNode = tokenAtNode.getTokenDataAtNode();
    Assert.assertEquals(1, tokenDataAtNode.size());
    Assert.assertEquals(AppWithWorkflow.DummyAction.TOKEN_VALUE,
                        tokenDataAtNode.get(AppWithWorkflow.DummyAction.TOKEN_KEY));
  }

  private WorkflowTokenDetail getWorkflowToken(Id.Workflow workflowId, String runId,
                                               @Nullable WorkflowToken.Scope scope,
                                               @Nullable String key) throws Exception {
    String workflowTokenUrl = String.format("apps/%s/workflows/%s/runs/%s/token", workflowId.getApplicationId(),
                                            workflowId.getId(), runId);
    String versionedUrl = getVersionedAPIPath(appendScopeAndKeyToUrl(workflowTokenUrl, scope, key),
                                              Constants.Gateway.API_VERSION_3_TOKEN, workflowId.getNamespaceId());
    HttpResponse response = doGet(versionedUrl);
    return readResponse(response, new TypeToken<WorkflowTokenDetail>() { }.getType(), GSON);
  }

  private WorkflowTokenNodeDetail getWorkflowToken(Id.Workflow workflowId, String runId, String nodeName,
                                                   @Nullable WorkflowToken.Scope scope,
                                                   @Nullable String key) throws Exception {
    String workflowTokenUrl = String.format("apps/%s/workflows/%s/runs/%s/nodes/%s/token",
                                            workflowId.getApplicationId(), workflowId.getId(), runId, nodeName);
    String versionedUrl = getVersionedAPIPath(appendScopeAndKeyToUrl(workflowTokenUrl, scope, key),
                                              Constants.Gateway.API_VERSION_3_TOKEN, workflowId.getNamespaceId());
    HttpResponse response = doGet(versionedUrl);
    return readResponse(response, new TypeToken<WorkflowTokenNodeDetail>() { }.getType(), GSON);
  }

  private String appendScopeAndKeyToUrl(String workflowTokenUrl, @Nullable WorkflowToken.Scope scope, String key) {
    StringBuilder output = new StringBuilder(workflowTokenUrl);
    if (scope != null) {
      output.append(String.format("?scope=%s", scope.name()));
      if (key != null) {
        output.append(String.format("&key=%s", key));
      }
    } else if (key != null) {
      output.append(String.format("?key=%s", key));
    }
    return output.toString();
  }

  private String createInputForRecordVerification(String folderName) throws IOException {
    File inputDir = tmpFolder.newFolder(folderName);

    File inputFile = new File(inputDir.getPath() + "/words.txt");
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile.toPath(), Charsets.UTF_8)) {
      writer.write("id1:value1");
      writer.newLine();
      writer.write("id2:value2");
      writer.newLine();
      writer.write("id3:value3");
    }
    return inputDir.getAbsolutePath();
  }

  @Test
  public void testWorkflowTokenPut() throws Exception {
    Assert.assertEquals(200, deploy(WorkflowTokenTestPutApp.class).getStatusLine().getStatusCode());
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, WorkflowTokenTestPutApp.NAME);
    Id.Workflow workflowId = Id.Workflow.from(appId, WorkflowTokenTestPutApp.WorkflowTokenTestPut.NAME);
    Id.Program mapReduceId = Id.Program.from(appId, ProgramType.MAPREDUCE, WorkflowTokenTestPutApp.RecordCounter.NAME);
    Id.Program sparkId = Id.Program.from(appId, ProgramType.SPARK, WorkflowTokenTestPutApp.SparkTestApp.NAME);

    // Start program with inputPath and outputPath arguments.
    // This should succeed. The programs inside the workflow will attempt to write to the workflow token
    // from the Mapper's and Reducer's methods as well as from a Spark closure, and they will throw an exception
    // if that succeeds.
    // The MapReduce's initialize will record the workflow run id in the token, and the destroy as well
    // as the mapper and the reducer will validate that they have the same workflow run id.
    String outputPath = new File(tmpFolder.newFolder(), "output").getAbsolutePath();
    startProgram(workflowId, ImmutableMap.of("inputPath", createInputForRecordVerification("sixthInput"),
                                             "outputPath", outputPath));

    waitState(workflowId, ProgramStatus.RUNNING.name());
    waitState(workflowId, ProgramStatus.STOPPED.name());

    // validate the completed workflow run and validate that it is the same as recorded in the token
    verifyProgramRuns(workflowId, "completed");
    List<RunRecord> runs = getProgramRuns(workflowId, "completed");
    Assert.assertEquals(1, runs.size());
    String wfRunId = runs.get(0).getPid();
    WorkflowTokenDetail tokenDetail = getWorkflowToken(workflowId, wfRunId, null, null);
    List<WorkflowTokenDetail.NodeValueDetail> details = tokenDetail.getTokenData().get("wf.runid");
    Assert.assertEquals(1, details.size());
    Assert.assertEquals(wfRunId, details.get(0).getValue());

    // validate that none of the mapper, reducer or spark closure were able to write to the token
    for (String key : new String[] {
      "mapper.initialize.key", "map.key", "reducer.initialize.key", "reduce.key", "some.key" }) {
      Assert.assertFalse(tokenDetail.getTokenData().containsKey(key));
    }

    List<RunRecord> sparkProgramRuns = getProgramRuns(sparkId, ProgramRunStatus.COMPLETED.name());
    Assert.assertEquals(1, sparkProgramRuns.size());
  }

  @Ignore
  @Test
  public void testWorkflowForkFailure() throws Exception {
    // Deploy an application containing workflow with fork. Fork executes MapReduce programs
    // 'FirstMapReduce' and 'SecondMapReduce' in parallel. Workflow is started with runtime argument
    // "mapreduce.SecondMapReduce.throw.exception", so that the MapReduce program 'SecondMapReduce'
    // fails. This causes the 'FirstMapReduce' program to get killed and Workflow is marked as failed.
    Assert.assertEquals(200, deploy(WorkflowFailureInForkApp.class).getStatusLine().getStatusCode());
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, WorkflowFailureInForkApp.NAME);
    Id.Workflow workflowId = Id.Workflow.from(appId, WorkflowFailureInForkApp.WorkflowWithFailureInFork.NAME);
    Id.Program firstMRId = Id.Program.from(appId, ProgramType.MAPREDUCE,
                                           WorkflowFailureInForkApp.FIRST_MAPREDUCE_NAME);
    Id.Program secondMRId = Id.Program.from(appId, ProgramType.MAPREDUCE,
                                            WorkflowFailureInForkApp.SECOND_MAPREDUCE_NAME);

    String outputPath = new File(tmpFolder.newFolder(), "output").getAbsolutePath();
    File fileToSync = new File(tmpFolder.newFolder() + "/sync.file");
    File fileToWait = new File(tmpFolder.newFolder() + "/wait.file");
    startProgram(workflowId, ImmutableMap.of("inputPath", createInput("testWorkflowForkFailureInput"),
                                             "outputPath", outputPath,
                                             "sync.file", fileToSync.getAbsolutePath(),
                                             "wait.file", fileToWait.getAbsolutePath(),
                                             "mapreduce." + WorkflowFailureInForkApp.SECOND_MAPREDUCE_NAME
                                               + ".throw.exception", "true"));
    waitState(workflowId, ProgramStatus.RUNNING.name());
    waitState(workflowId, ProgramStatus.STOPPED.name());

    verifyProgramRuns(workflowId, "failed");

    List<RunRecord> mapReduceProgramRuns = getProgramRuns(firstMRId, ProgramRunStatus.KILLED.name());
    Assert.assertEquals(1, mapReduceProgramRuns.size());

    mapReduceProgramRuns = getProgramRuns(secondMRId, ProgramRunStatus.FAILED.name());
    Assert.assertEquals(1, mapReduceProgramRuns.size());
  }
}
