/*
 * Copyright © 2014-2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services.http.handlers;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.AppWithMultipleSchedules;
import io.cdap.cdap.AppWithSchedule;
import io.cdap.cdap.AppWithServices;
import io.cdap.cdap.AppWithWorker;
import io.cdap.cdap.AppWithWorkflow;
import io.cdap.cdap.DummyAppWithTrackingTable;
import io.cdap.cdap.SleepingWorkflowApp;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.schedule.SchedulableProgramType;
import io.cdap.cdap.api.service.ServiceSpecification;
import io.cdap.cdap.api.service.http.HttpServiceHandlerSpecification;
import io.cdap.cdap.api.service.http.ServiceHttpEndpoint;
import io.cdap.cdap.api.workflow.ScheduleProgramInfo;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.gateway.handlers.ProgramLifecycleHttpHandler;
import io.cdap.cdap.internal.app.ServiceSpecificationCodec;
import io.cdap.cdap.internal.app.deploy.Specifications;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import io.cdap.cdap.internal.app.runtime.schedule.constraint.ConcurrencyConstraint;
import io.cdap.cdap.internal.app.runtime.schedule.store.Schedulers;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.OrTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.PartitionTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.provision.MockProvisioner;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.BatchProgramHistory;
import io.cdap.cdap.proto.Instances;
import io.cdap.cdap.proto.ProgramRecord;
import io.cdap.cdap.proto.ProgramRunClusterStatus;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.ProtoConstraint;
import io.cdap.cdap.proto.ProtoTrigger;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.ServiceInstances;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ServiceId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.test.SlowTests;
import io.cdap.cdap.test.XSlowTests;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Tests for {@link ProgramLifecycleHttpHandler}
 */
public class ProgramLifecycleHttpHandlerTest extends AppFabricTestBase {

  private static final Gson GSON = new GsonBuilder()
    .create();
  private static final Type LIST_OF_JSONOBJECT_TYPE = new TypeToken<List<JsonObject>>() { }.getType();
  private static final Type LIST_OF_RUN_RECORD = new TypeToken<List<RunRecord>>() { }.getType();

  private static final String DUMMY_APP_ID = "dummy";
  private static final String DUMMY_MR_NAME = "dummy-batch";
  private static final String SLEEP_WORKFLOW_APP_ID = "SleepWorkflowApp";
  private static final String SLEEP_WORKFLOW_NAME = "SleepWorkflow";

  private static final String EMPTY_ARRAY_JSON = "[]";
  private static final String STOPPED = "STOPPED";
  private static final String RUNNING = "RUNNING";

  @Category(XSlowTests.class)
  @Test
  public void testProgramStartStopStatus() throws Exception {
    // deploy, check the status
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);

    ProgramId serviceId1 = new ServiceId(TEST_NAMESPACE1, AllProgramsApp.NAME, AllProgramsApp.NoOpService.NAME);
    ProgramId serviceId2 = new ServiceId(TEST_NAMESPACE2, AllProgramsApp.NAME, AllProgramsApp.NoOpService.NAME);

    // service is stopped initially
    Assert.assertEquals(STOPPED, getProgramStatus(serviceId1));

    // start service in the wrong namespace and verify that it does not start
    startProgram(serviceId2, 404);

    // start a service and check the status
    startProgram(serviceId1);
    waitState(serviceId1, RUNNING);

    // stop the service and check the status
    stopProgram(serviceId1);
    waitState(serviceId1, STOPPED);

    // deploy another app in a different namespace and verify
    deploy(DummyAppWithTrackingTable.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);

    Id.Program dummyMR1 = Id.Program.from(TEST_NAMESPACE1, DUMMY_APP_ID, ProgramType.MAPREDUCE, DUMMY_MR_NAME);
    Id.Program dummyMR2 = Id.Program.from(TEST_NAMESPACE2, DUMMY_APP_ID, ProgramType.MAPREDUCE, DUMMY_MR_NAME);

    // mapreduce is stopped initially
    Assert.assertEquals(STOPPED, getProgramStatus(dummyMR2));

    // start mapreduce in the wrong namespace and verify it does not start
    startProgram(dummyMR1, 404);
    Assert.assertEquals(STOPPED, getProgramStatus(dummyMR2));

    // start map-reduce and verify status
    startProgram(dummyMR2);
    waitState(dummyMR2, RUNNING);

    // stop the mapreduce program and check the status
    stopProgram(dummyMR2);
    waitState(dummyMR2, STOPPED);

    // start multiple runs of the map-reduce program
    startProgram(dummyMR2);
    startProgram(dummyMR2);
    verifyProgramRuns(dummyMR2, ProgramRunStatus.RUNNING, 1);

    // stop all runs of the map-reduce program
    stopProgram(dummyMR2, 200);
    waitState(dummyMR2, STOPPED);

    // deploy an app containing a workflow
    deploy(SleepingWorkflowApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);

    Id.Program sleepWorkflow1 =
      Id.Program.from(TEST_NAMESPACE1, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW, SLEEP_WORKFLOW_NAME);
    Id.Program sleepWorkflow2 =
      Id.Program.from(TEST_NAMESPACE2, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW, SLEEP_WORKFLOW_NAME);

    // workflow is stopped initially
    Assert.assertEquals(STOPPED, getProgramStatus(sleepWorkflow2));

    // start workflow in the wrong namespace and verify that it does not start
    startProgram(sleepWorkflow1, 404);
    Assert.assertEquals(STOPPED, getProgramStatus(sleepWorkflow2));

    // start workflow and check status
    startProgram(sleepWorkflow2);
    waitState(sleepWorkflow2, RUNNING);

    // workflow will stop itself
    waitState(sleepWorkflow2, STOPPED);

    // start multiple runs of the workflow
    startProgram(sleepWorkflow2, ImmutableMap.of("sleep.ms", "5000"));
    startProgram(sleepWorkflow2, ImmutableMap.of("sleep.ms", "5000"));
    verifyProgramRuns(sleepWorkflow2, ProgramRunStatus.RUNNING, 1);

    List<RunRecord> runs = getProgramRuns(sleepWorkflow2, ProgramRunStatus.RUNNING);
    Assert.assertEquals(2, runs.size());
    stopProgram(sleepWorkflow2, runs.get(0).getPid(), 200);
    stopProgram(sleepWorkflow2, runs.get(1).getPid(), 200);
    waitState(sleepWorkflow2, STOPPED);

    long startTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    // Set super long sleep so we can suspend the workflow ourselves.
    startProgram(sleepWorkflow2, ImmutableMap.of("sleep.ms", "500000"));
    waitState(sleepWorkflow2, RUNNING);
    stopProgram(sleepWorkflow2);
    waitState(sleepWorkflow2, STOPPED);
    long endTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) + 1;

    // sleepWorkflow2 should be restarted
    restartPrograms(new ApplicationId(TEST_NAMESPACE2, SLEEP_WORKFLOW_APP_ID), startTime, endTime);
    waitState(sleepWorkflow2, RUNNING);

    stopProgram(sleepWorkflow2);
    waitState(sleepWorkflow2, STOPPED);

    // verify batch runs endpoint
    List<ProgramId> programs = ImmutableList.of(sleepWorkflow2.toEntityId(), dummyMR2.toEntityId(),
                                                serviceId2);
    List<BatchProgramHistory> batchRuns = getProgramRuns(new NamespaceId(TEST_NAMESPACE2), programs);
    BatchProgramHistory sleepRun = batchRuns.get(0);
    BatchProgramHistory dummyMR2Run = batchRuns.get(1);
    BatchProgramHistory service2Run = batchRuns.get(2);

    // verify results come back in order
    Assert.assertEquals(sleepWorkflow2.getId(), sleepRun.getProgramId());
    Assert.assertEquals(dummyMR2.getId(), dummyMR2Run.getProgramId());
    Assert.assertEquals(serviceId2.getProgram(), service2Run.getProgramId());

    // verify status. AllProgramsApp was never deployed in NS2 and should not exist
    Assert.assertEquals(200, sleepRun.getStatusCode());
    Assert.assertEquals(200, dummyMR2Run.getStatusCode());
    Assert.assertEquals(404, service2Run.getStatusCode());

    // verify the run record is correct
    RunRecord runRecord = getProgramRuns(sleepWorkflow2, ProgramRunStatus.ALL).iterator().next();
    Assert.assertEquals(runRecord.getPid(), sleepRun.getRuns().iterator().next().getPid());

    runRecord = getProgramRuns(dummyMR2, ProgramRunStatus.ALL).iterator().next();
    Assert.assertEquals(runRecord.getPid(), dummyMR2Run.getRuns().iterator().next().getPid());

    Assert.assertTrue(service2Run.getRuns().isEmpty());

    // cleanup
    HttpResponse response = doDelete(getVersionedAPIPath("apps/",
                                                         Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getResponseCode());
    response = doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2));
    Assert.assertEquals(200, response.getResponseCode());
  }

  @Test
  public void testVersionedProgramStartStopStatus() throws Exception {
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "app", VERSION1);
    addAppArtifact(artifactId, AllProgramsApp.class);
    AppRequest<? extends Config> appRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()));

    ApplicationId appId1 = NamespaceId.DEFAULT.app(AllProgramsApp.NAME, VERSION1);
    ApplicationId appId2 = NamespaceId.DEFAULT.app(AllProgramsApp.NAME, VERSION2);
    Id.Application appDefault = Id.Application.fromEntityId(appId1);

    // deploy app1
    Assert.assertEquals(200, deploy(appId1, appRequest).getResponseCode());

    // deploy app1 with default version
    Assert.assertEquals(200, deploy(appDefault, appRequest).getResponseCode());

    // deploy the second version of the app
    Assert.assertEquals(200, deploy(appId2, appRequest).getResponseCode());

    ProgramId serviceId1 = appId1.program(ProgramType.SERVICE, AllProgramsApp.NoOpService.NAME);
    ProgramId serviceId2 = appId2.program(ProgramType.SERVICE, AllProgramsApp.NoOpService.NAME);
    Id.Program serviceIdDefault = Id.Program.fromEntityId(serviceId1);
    // service is stopped initially
    Assert.assertEquals(STOPPED, getProgramStatus(serviceId1));
    // start service
    startProgram(serviceId1, 200);
    waitState(serviceId1, RUNNING);
    // wordFrequencyService2 is stopped initially
    Assert.assertEquals(STOPPED, getProgramStatus(serviceId2));
    // start service in version2
    startProgram(serviceId2, 200);
    waitState(serviceId2, RUNNING);
    // wordFrequencyServiceDefault is stopped initially
    Assert.assertEquals(STOPPED, getProgramStatus(serviceIdDefault));
    // start service in default version
    startProgram(serviceIdDefault, 200);
    waitState(serviceIdDefault, RUNNING);
    // same service cannot be run concurrently in the same app version
    startProgram(serviceId1, 409);
    stopProgram(serviceId1, null, 200, null);
    waitState(serviceId1, STOPPED);
    Assert.assertEquals(STOPPED, getProgramStatus(serviceId1));
    // wordFrequencyService1 can be run after wordFrequencyService1 is stopped
    startProgram(serviceId1, 200);
    waitState(serviceId1, RUNNING);

    stopProgram(serviceId1, null, 200, null);
    stopProgram(serviceId2, null, 200, null);
    stopProgram(serviceIdDefault, null, 200, null);
    waitState(serviceId1, STOPPED);
    waitState(serviceId2, STOPPED);
    waitState(serviceIdDefault, STOPPED);

    Id.Artifact sleepWorkflowArtifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "sleepworkflowapp", VERSION1);
    addAppArtifact(sleepWorkflowArtifactId, SleepingWorkflowApp.class);
    AppRequest<? extends Config> sleepWorkflowRequest = new AppRequest<>(
      new ArtifactSummary(sleepWorkflowArtifactId.getName(), sleepWorkflowArtifactId.getVersion().getVersion()));

    ApplicationId sleepWorkflowApp1 = new ApplicationId(Id.Namespace.DEFAULT.getId(), "SleepingWorkflowApp", VERSION1);
    final ProgramId sleepWorkflow1 = sleepWorkflowApp1.program(ProgramType.WORKFLOW, "SleepWorkflow");

    ApplicationId sleepWorkflowApp2 = new ApplicationId(Id.Namespace.DEFAULT.getId(), "SleepingWorkflowApp", VERSION2);
    final ProgramId sleepWorkflow2 = sleepWorkflowApp2.program(ProgramType.WORKFLOW, "SleepWorkflow");

    // Start wordCountApp1
    Assert.assertEquals(200, deploy(sleepWorkflowApp1, sleepWorkflowRequest).getResponseCode());
    // workflow is stopped initially
    Assert.assertEquals(STOPPED, getProgramStatus(sleepWorkflow1));
    // start workflow in a wrong version
    startProgram(sleepWorkflow2, 404);
    // Start wordCountApp2
    Assert.assertEquals(200, deploy(sleepWorkflowApp2, sleepWorkflowRequest).getResponseCode());

    // start multiple workflow simultaneously with a long sleep time
    Map<String, String> args = Collections.singletonMap("sleep.ms", "120000");
    startProgram(sleepWorkflow1, args, 200);
    startProgram(sleepWorkflow2, args, 200);
    startProgram(sleepWorkflow1, args, 200);
    startProgram(sleepWorkflow2, args, 200);

    // Make sure they are all running. Otherwise on slow machine, it's possible that the TMS states hasn't
    // been consumed and write to the store before we stop the program and query for STOPPED state.
    Tasks.waitFor(2, () -> getProgramRuns(sleepWorkflow1, ProgramRunStatus.RUNNING).size(),
                  10, TimeUnit.SECONDS, 200, TimeUnit.MILLISECONDS);
    Tasks.waitFor(2, () -> getProgramRuns(sleepWorkflow2, ProgramRunStatus.RUNNING).size(),
                  10, TimeUnit.SECONDS, 200, TimeUnit.MILLISECONDS);

    // stop multiple workflow simultaneously
    // This will stop all concurrent runs of the Workflow version 1.0.0
    stopProgram(sleepWorkflow1, null, 200, null);
    // This will stop all concurrent runs of the Workflow version 2.0.0
    stopProgram(sleepWorkflow2, null, 200, null);

    // Wait until all are stopped
    waitState(sleepWorkflow1, STOPPED);
    waitState(sleepWorkflow2, STOPPED);

    //Test for runtime args
    testVersionedProgramRuntimeArgs(sleepWorkflow1);

    // cleanup
    deleteApp(appId1, 200);
    deleteApp(appId2, 200);
    deleteApp(appDefault, 200);
    deleteApp(sleepWorkflowApp1, 200);
    deleteApp(sleepWorkflowApp2, 200);
  }

  @Category(XSlowTests.class)
  @Test
  public void testProgramStartStopStatusErrors() throws Exception {
    // deploy, check the status
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);

    String appName = AllProgramsApp.NAME;
    String serviceName = AllProgramsApp.NoOpService.NAME;
    String mrName = AllProgramsApp.NoOpMR.NAME;

    // start unknown program
    startProgram(Id.Program.from(TEST_NAMESPACE1, appName, ProgramType.SERVICE, "noexist"), 404);
    // start program in unknonw app
    startProgram(Id.Program.from(TEST_NAMESPACE1, "noexist", ProgramType.SERVICE, serviceName), 404);
    // start program in unknown namespace
    startProgram(Id.Program.from("noexist", appName, ProgramType.SERVICE, serviceName), 404);

    // debug unknown program
    debugProgram(Id.Program.from(TEST_NAMESPACE1, appName, ProgramType.SERVICE, "noexist"), 404);
    // debug a program that does not support it
    debugProgram(Id.Program.from(TEST_NAMESPACE1, appName, ProgramType.MAPREDUCE, mrName),
                 501); // not implemented

    // status for unknown program
    programStatus(Id.Program.from(TEST_NAMESPACE1, appName, ProgramType.SERVICE, "noexist"), 404);
    // status for program in unknonw app
    programStatus(Id.Program.from(TEST_NAMESPACE1, "noexist", ProgramType.SERVICE, serviceName), 404);
    // status for program in unknown namespace
    programStatus(Id.Program.from("noexist", appName, ProgramType.SERVICE, serviceName), 404);

    // stop unknown program
    stopProgram(Id.Program.from(TEST_NAMESPACE1, appName, ProgramType.SERVICE, "noexist"), 404);
    // stop program in unknonw app
    stopProgram(Id.Program.from(TEST_NAMESPACE1, "noexist", ProgramType.SERVICE, serviceName), 404);
    // stop program in unknown namespace
    stopProgram(Id.Program.from("noexist", appName, ProgramType.SERVICE, serviceName), 404);
    // stop program that is not running
    stopProgram(Id.Program.from(TEST_NAMESPACE1, appName, ProgramType.SERVICE, serviceName), 400);
    // stop run of a program with ill-formed run id
    stopProgram(Id.Program.from(TEST_NAMESPACE1, appName, ProgramType.SERVICE, serviceName),
                "norunid", 400);

    // start program twice
    startProgram(Id.Program.from(TEST_NAMESPACE1, appName, ProgramType.SERVICE, serviceName));
    verifyProgramRuns(Id.Program.from(TEST_NAMESPACE1, appName, ProgramType.SERVICE, serviceName),
                      ProgramRunStatus.RUNNING);

    startProgram(Id.Program.from(TEST_NAMESPACE1, appName, ProgramType.SERVICE, serviceName),
                 409); // conflict

    // get run records for later use
    List<RunRecord> runs = getProgramRuns(
      Id.Program.from(TEST_NAMESPACE1, appName, ProgramType.SERVICE, serviceName), ProgramRunStatus.RUNNING);
    Assert.assertEquals(1, runs.size());
    String runId = runs.get(0).getPid();

    // stop program
    stopProgram(Id.Program.from(TEST_NAMESPACE1, appName, ProgramType.SERVICE, serviceName), 200);
    waitState(Id.Program.from(TEST_NAMESPACE1, appName, ProgramType.SERVICE, serviceName), "STOPPED");

    // get run records again, should be empty now
    Tasks.waitFor(true, () -> {
      Id.Program id = Id.Program.from(TEST_NAMESPACE1, appName, ProgramType.SERVICE, serviceName);
      return getProgramRuns(id, ProgramRunStatus.RUNNING).isEmpty();
    }, 10, TimeUnit.SECONDS);

    // stop run of the program that is not running
    stopProgram(Id.Program.from(TEST_NAMESPACE1, appName, ProgramType.SERVICE, serviceName),
                runId, 400); // active run not found

    // cleanup
    HttpResponse response = doDelete(getVersionedAPIPath("apps/",
                                                         Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getResponseCode());
  }

  /**
   * Tests history of a mapreduce.
   */
  @Category(XSlowTests.class)
  @Test
  public void testMapreduceHistory() throws Exception {
    testHistory(DummyAppWithTrackingTable.class,
                Id.Program.from(TEST_NAMESPACE2, DUMMY_APP_ID, ProgramType.MAPREDUCE, DUMMY_MR_NAME));
  }

  /**
   * Tests history of a non existing program
   */
  @Test
  public void testNonExistingProgramHistory() throws Exception {
    deploy(DummyAppWithTrackingTable.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);
    int historyStatus = doPost(getVersionedAPIPath("apps/" + DUMMY_APP_ID + ProgramType.MAPREDUCE + "/NonExisting",
                                                   Constants.Gateway.API_VERSION_3_TOKEN,
                                                   TEST_NAMESPACE2)).getResponseCode();
    int deleteStatus = doDelete(getVersionedAPIPath("apps/" + DUMMY_APP_ID, Constants.Gateway.API_VERSION_3_TOKEN,
                                                    TEST_NAMESPACE2)).getResponseCode();
    Assert.assertTrue("Unexpected history status " + historyStatus + " and/or deleteStatus " + deleteStatus,
                      historyStatus == 404 && deleteStatus == 200);
  }

  /**
   * Tests getting a non-existent namespace
   */
  @Test
  public void testNonExistentNamespace() throws Exception {
    String[] endpoints = {"spark", "services", "workers", "mapreduce", "workflows"};

    for (String endpoint : endpoints) {
      HttpResponse response = doGet("/v3/namespaces/default/" + endpoint);
      Assert.assertEquals(200, response.getResponseCode());
      response = doGet("/v3/namespaces/garbage/" + endpoint);
      Assert.assertEquals(404, response.getResponseCode());
    }
  }

  /**
   * Tests history of a workflow.
   */
  @Category(SlowTests.class)
  @Test
  public void testWorkflowHistory() throws Exception {
    deploy(SleepingWorkflowApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Id.Program sleepWorkflow1 =
      Id.Program.from(TEST_NAMESPACE1, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW, SLEEP_WORKFLOW_NAME);

    // first run
    startProgram(sleepWorkflow1);
    int numWorkflowRunsStopped = getProgramRuns(sleepWorkflow1, ProgramRunStatus.COMPLETED).size();
    // workflow stops by itself after actions are done
    waitState(sleepWorkflow1, STOPPED);
    verifyProgramRuns(sleepWorkflow1, ProgramRunStatus.COMPLETED, numWorkflowRunsStopped);

    // second run
    startProgram(sleepWorkflow1);
    // workflow stops by itself after actions are done
    waitState(sleepWorkflow1, STOPPED);
    verifyProgramRuns(sleepWorkflow1, ProgramRunStatus.COMPLETED, numWorkflowRunsStopped + 1);

    historyStatusWithRetry(sleepWorkflow1.toEntityId(), ProgramRunStatus.COMPLETED, 2);

    deleteApp(sleepWorkflow1.getApplication(), 200);
  }

  @Test
  public void testProvisionerFailureStateAndMetrics() throws Exception {
    // test that metrics and program state are correct after a program run fails due to provisioning failures
    deploy(SleepingWorkflowApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Id.Program workflowId =
      Id.Program.from(TEST_NAMESPACE1, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW, SLEEP_WORKFLOW_NAME);

    // get number of failed runs and metrics
    long failMetricCount = getProfileTotalMetric(Constants.Metrics.Program.PROGRAM_FAILED_RUNS);
    int numFailedRuns = getProgramRuns(workflowId, ProgramRunStatus.FAILED).size();

    // this tells the provisioner to fail the create call
    Map<String, String> args = new HashMap<>();
    args.put(SystemArguments.PROFILE_PROPERTIES_PREFIX + MockProvisioner.FAIL_CREATE, Boolean.TRUE.toString());
    startProgram(workflowId, args);

    Tasks.waitFor(numFailedRuns + 1, () -> getProgramRuns(workflowId, ProgramRunStatus.FAILED).size(),
                  5, TimeUnit.MINUTES);

    // check program state and cluster state
    Tasks.waitFor(ProgramRunClusterStatus.DEPROVISIONED, () -> {
      RunRecord runRecord = getProgramRuns(workflowId, ProgramRunStatus.FAILED).iterator().next();
      return runRecord.getCluster().getStatus();
    }, 1, TimeUnit.MINUTES);

    // check profile metrics. Though not guaranteed to be set when the program is done, it should be set soon after.
    Tasks.waitFor(failMetricCount + 1, () -> getProfileTotalMetric(Constants.Metrics.Program.PROGRAM_FAILED_RUNS),
                  60, TimeUnit.SECONDS);
    deleteApp(workflowId.getApplication(), 200);
  }

  @Test
  public void testStopProgramWhilePending() throws Exception {
    deploy(SleepingWorkflowApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Id.Program workflowId =
      Id.Program.from(TEST_NAMESPACE1, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW, SLEEP_WORKFLOW_NAME);

    int numKilledRuns = getProgramRuns(workflowId, ProgramRunStatus.KILLED).size();

    // this tells the provisioner to wait for 60s before trying to create the cluster for the run
    Map<String, String> args = new HashMap<>();
    args.put(SystemArguments.PROFILE_PROPERTIES_PREFIX + MockProvisioner.WAIT_CREATE_MS, Integer.toString(120000));
    startProgram(workflowId, args);

    // should be safe to wait for starting since the provisioner is configure to sleep while creating a cluster
    waitState(workflowId, io.cdap.cdap.proto.ProgramStatus.STARTING.name());

    stopProgram(workflowId);
    waitState(workflowId, STOPPED);

    verifyProgramRuns(workflowId, ProgramRunStatus.KILLED, numKilledRuns);

    deleteApp(workflowId.getApplication(), 200);
  }

  @Test
  public void testStopProgramRunWhilePending() throws Exception {
    deploy(SleepingWorkflowApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Id.Program workflowId =
      Id.Program.from(TEST_NAMESPACE1, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW, SLEEP_WORKFLOW_NAME);

    int numKilledRuns = getProgramRuns(workflowId, ProgramRunStatus.KILLED).size();

    // this tells the provisioner to wait for 60s before trying to create the cluster for the run
    Map<String, String> args = new HashMap<>();
    args.put(SystemArguments.PROFILE_PROPERTIES_PREFIX + MockProvisioner.WAIT_CREATE_MS, Integer.toString(120000));
    startProgram(workflowId, args);

    // should be safe to wait for starting since the provisioner is configure to sleep while creating a cluster
    waitState(workflowId, io.cdap.cdap.proto.ProgramStatus.STARTING.name());
    List<RunRecord> runRecords = getProgramRuns(workflowId, ProgramRunStatus.PENDING);
    Assert.assertEquals(1, runRecords.size());
    String runId = runRecords.iterator().next().getPid();

    stopProgram(workflowId, runId, 200);
    waitState(workflowId, STOPPED);

    verifyProgramRuns(workflowId, ProgramRunStatus.KILLED, numKilledRuns);
    deleteApp(workflowId.getApplication(), 200);
  }

  @Test
  public void testWorkflowRuntimeArgs() throws Exception {
    testRuntimeArgs(SleepingWorkflowApp.class, TEST_NAMESPACE2, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW
      .getCategoryName(), SLEEP_WORKFLOW_NAME);
  }

  @Test
  public void testMapreduceRuntimeArgs() throws Exception {
    testRuntimeArgs(DummyAppWithTrackingTable.class, TEST_NAMESPACE1, DUMMY_APP_ID, ProgramType.MAPREDUCE
      .getCategoryName(), DUMMY_MR_NAME);
  }

  @Test
  public void testBatchStatus() throws Exception {
    final String statusUrl1 = getVersionedAPIPath("status", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    final String statusUrl2 = getVersionedAPIPath("status", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);

    // invalid json must return 400
    Assert.assertEquals(400, doPost(statusUrl1, "").getResponseCode());
    Assert.assertEquals(400, doPost(statusUrl2, "").getResponseCode());
    // empty array is valid args
    Assert.assertEquals(200, doPost(statusUrl1, EMPTY_ARRAY_JSON).getResponseCode());
    Assert.assertEquals(200, doPost(statusUrl2, EMPTY_ARRAY_JSON).getResponseCode());

    // deploy an app in namespace1
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    // deploy another app in namespace2
    deploy(AppWithServices.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);

    Gson gson = new Gson();

    // data requires appId, programId, and programType. Test missing fields/invalid programType
    List<Map<String, String>> request = Collections.singletonList(
      ImmutableMap.of("appId", AllProgramsApp.NAME, "programType", "Service")
    );
    Assert.assertEquals(400, doPost(statusUrl1, gson.toJson(request)).getResponseCode());

    request = Collections.singletonList(
      ImmutableMap.of("appId", AllProgramsApp.NAME, "programId", AllProgramsApp.NoOpService.NAME)
    );
    Assert.assertEquals(400, doPost(statusUrl1, gson.toJson(request)).getResponseCode());

    request = Arrays.asList(
      ImmutableMap.of("programType", "Service", "programId", AllProgramsApp.NoOpService.NAME),
      ImmutableMap.of("appId", AppWithServices.NAME, "programType", "service",
                      "programId", AppWithServices.SERVICE_NAME)
    );
    Assert.assertEquals(400, doPost(statusUrl1, gson.toJson(request)).getResponseCode());

    request = Collections.singletonList(
      ImmutableMap.of("appId", AllProgramsApp.NAME, "programType", "XXX")
    );
    Assert.assertEquals(400, doPost(statusUrl1, gson.toJson(request)).getResponseCode());

    // Test missing app, programType, etc
    request = Collections.singletonList(
      ImmutableMap.of("appId", "NotExist", "programType", ProgramType.SERVICE.getPrettyName(), "programId", "Service")
    );
    List<JsonObject> returnedBody = readResponse(doPost(statusUrl1, gson.toJson(request)), LIST_OF_JSONOBJECT_TYPE);
    Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, returnedBody.get(0).get("statusCode").getAsInt());

    request = Arrays.asList(
      ImmutableMap.of("appId", AllProgramsApp.NAME, "programType", ProgramType.SERVICE.getPrettyName(),
                      "programId", "NotExist"),
      ImmutableMap.of("appId", AllProgramsApp.NAME, "programType", ProgramType.SERVICE.getPrettyName(),
                      "programId", AllProgramsApp.NoOpService.NAME)
    );
    returnedBody = readResponse(doPost(statusUrl1, gson.toJson(request)), LIST_OF_JSONOBJECT_TYPE);
    Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, returnedBody.get(0).get("statusCode").getAsInt());

    // The programType should be consistent. Second object should have proper status
    Assert.assertEquals("Service", returnedBody.get(1).get("programType").getAsString());
    Assert.assertEquals(STOPPED, returnedBody.get(1).get("status").getAsString());


    // test valid cases for namespace1
    request = Arrays.asList(
      ImmutableMap.of("appId", AllProgramsApp.NAME, "programType", "Service",
                      "programId", AllProgramsApp.NoOpService.NAME),
      ImmutableMap.of("appId", AllProgramsApp.NAME, "programType", "Worker",
                      "programId", AllProgramsApp.NoOpWorker.NAME)
    );
    HttpResponse response = doPost(statusUrl1, gson.toJson(request));
    verifyInitialBatchStatusOutput(response);

    // test valid cases for namespace2
    request = Collections.singletonList(
      ImmutableMap.of("appId", AppWithServices.NAME, "programType", "Service",
                      "programId", AppWithServices.SERVICE_NAME)
    );
    response = doPost(statusUrl2, gson.toJson(request));
    verifyInitialBatchStatusOutput(response);

    // start the service in ns1
    ServiceId serviceId1 = new ServiceId(TEST_NAMESPACE1, AllProgramsApp.NAME, AllProgramsApp.NoOpService.NAME);
    ServiceId serviceId2 = new ServiceId(TEST_NAMESPACE2, AppWithServices.NAME, AppWithServices.SERVICE_NAME);

    startProgram(serviceId1);
    waitState(serviceId1, RUNNING);

    // test status API after starting the service
    request = Arrays.asList(
      ImmutableMap.of("appId", AllProgramsApp.NAME, "programType", "Service",
                      "programId", AllProgramsApp.NoOpService.NAME),
      ImmutableMap.of("appId", AllProgramsApp.NAME, "programType", "MapReduce", "programId", AllProgramsApp.NoOpMR.NAME)
    );
    response = doPost(statusUrl1, gson.toJson(request));
    Assert.assertEquals(200, response.getResponseCode());
    returnedBody = readResponse(response, LIST_OF_JSONOBJECT_TYPE);
    Assert.assertEquals(ProgramRunStatus.RUNNING.toString(), returnedBody.get(0).get("status").getAsString());
    Assert.assertEquals(STOPPED, returnedBody.get(1).get("status").getAsString());

    // start the service in ns2
    startProgram(serviceId2);
    waitState(serviceId2, RUNNING);

    // test status API after starting the service
    request = Collections.singletonList(
      ImmutableMap.of("appId", AppWithServices.NAME, "programType", "Service",
                      "programId", AppWithServices.SERVICE_NAME)
    );
    response = doPost(statusUrl2, gson.toJson(request));
    Assert.assertEquals(200, response.getResponseCode());
    returnedBody = readResponse(response, LIST_OF_JSONOBJECT_TYPE);
    Assert.assertEquals(ProgramRunStatus.RUNNING.toString(), returnedBody.get(0).get("status").getAsString());

    // stop both services
    stopProgram(serviceId1);
    stopProgram(serviceId2);
    waitState(serviceId1, STOPPED);
    waitState(serviceId2, STOPPED);

    // try posting a status request with namespace2 for apps in namespace1
    request = Arrays.asList(
      ImmutableMap.of("appId", AllProgramsApp.NAME, "programType", "Worker",
                      "programId", AllProgramsApp.NoOpWorker.NAME),
      ImmutableMap.of("appId", AllProgramsApp.NAME, "programType", "Service",
                      "programId", AllProgramsApp.NoOpService.NAME),
      ImmutableMap.of("appId", AllProgramsApp.NAME, "programType", "Mapreduce", "programId", AllProgramsApp.NoOpMR.NAME)
    );
    response = doPost(statusUrl2, gson.toJson(request));
    returnedBody = readResponse(response, LIST_OF_JSONOBJECT_TYPE);
    Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, returnedBody.get(0).get("statusCode").getAsInt());
    Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, returnedBody.get(1).get("statusCode").getAsInt());
    Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, returnedBody.get(2).get("statusCode").getAsInt());
  }

  @Test
  public void testBatchInstances() throws Exception {
    String instancesUrl1 = getVersionedAPIPath("instances", Constants.Gateway.API_VERSION_3_TOKEN,
                                                     TEST_NAMESPACE1);

    Assert.assertEquals(400, doPost(instancesUrl1, "").getResponseCode());

    // empty array is valid args
    Assert.assertEquals(200, doPost(instancesUrl1, "[]").getResponseCode());

    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);

    Gson gson = new Gson();

    // data requires appId, programId, and programType. Test missing fields/invalid programType
    List<Map<String, String>> request = Collections.singletonList(
      ImmutableMap.of("appId", AllProgramsApp.NAME, "programType", "Service")
    );
    Assert.assertEquals(400, doPost(instancesUrl1, gson.toJson(request)).getResponseCode());

    request = Collections.singletonList(
      ImmutableMap.of("appId", AllProgramsApp.NAME, "programId", AllProgramsApp.NoOpService.NAME)
    );
    Assert.assertEquals(400, doPost(instancesUrl1, gson.toJson(request)).getResponseCode());

    request = Arrays.asList(
      ImmutableMap.of("programType", "Service", "programId", AllProgramsApp.NoOpService.NAME),
      ImmutableMap.of("appId", AllProgramsApp.NAME, "programType", "Service",
                      "programId", AllProgramsApp.NoOpService.NAME)
    );
    Assert.assertEquals(400, doPost(instancesUrl1, gson.toJson(request)).getResponseCode());

    request = Collections.singletonList(
      ImmutableMap.of("appId", AllProgramsApp.NAME, "programType", "XXX",
                      "programId", AllProgramsApp.NoOpService.NAME)
    );
    Assert.assertEquals(400, doPost(instancesUrl1, gson.toJson(request)).getResponseCode());

    // Test malformed json
    request = Collections.singletonList(
      ImmutableMap.of("appId", AllProgramsApp.NAME, "programType", "Service",
                      "programId", AllProgramsApp.NoOpService.NAME)
    );
    Assert.assertEquals(400, doPost(instancesUrl1, gson.toJson(request) + "]]").getResponseCode());

    // Test missing app
    request = Collections.singletonList(
      ImmutableMap.of("appId", "NotExist", "programType", "Service", "programId", AllProgramsApp.NoOpService.NAME)
    );
    List<JsonObject> returnedBody = readResponse(doPost(instancesUrl1, gson.toJson(request)), LIST_OF_JSONOBJECT_TYPE);
    Assert.assertEquals(404, returnedBody.get(0).get("statusCode").getAsInt());

    // valid test
    request = Arrays.asList(
      ImmutableMap.of("appId", AllProgramsApp.NAME, "programType", "Service",
                      "programId", AllProgramsApp.NoOpService.NAME),
      ImmutableMap.of("appId", AllProgramsApp.NAME, "programType", "Worker",
                      "programId", AllProgramsApp.NoOpWorker.NAME)
    );
    HttpResponse response = doPost(instancesUrl1, gson.toJson(request));
    verifyInitialBatchInstanceOutput(response);

    // start the service
    ServiceId serviceId = new ServiceId(TEST_NAMESPACE1, AllProgramsApp.NAME, AllProgramsApp.NoOpService.NAME);
    startProgram(serviceId);
    waitState(serviceId, RUNNING);

    request = Collections.singletonList(
      ImmutableMap.of("appId", AllProgramsApp.NAME, "programType", "Service",
                      "programId", AllProgramsApp.NoOpService.NAME)
    );
    response = doPost(instancesUrl1, gson.toJson(request));
    returnedBody = readResponse(response, LIST_OF_JSONOBJECT_TYPE);
    Assert.assertEquals(1, returnedBody.get(0).get("provisioned").getAsInt());

    // Increase service instances to 2
    String setInstanceUrl = getVersionedAPIPath(
      String.format("apps/%s/services/%s/instances", AllProgramsApp.NAME, AllProgramsApp.NoOpService.NAME),
      Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(200, doPut(setInstanceUrl, gson.toJson(new Instances(2))).getResponseCode());

    // Validate there are 2 instances
    Tasks.waitFor(2, () -> {
      List<Map<String, String>> request1 = Collections.singletonList(
        ImmutableMap.of("appId", AllProgramsApp.NAME, "programType", "Service",
                        "programId", AllProgramsApp.NoOpService.NAME)
      );
      HttpResponse response1 = doPost(instancesUrl1, gson.toJson(request1));
      List<JsonObject> returnedBody1 = readResponse(response1, LIST_OF_JSONOBJECT_TYPE);
      return returnedBody1.get(0).get("provisioned").getAsInt();
    }, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    stopProgram(serviceId);
    waitState(serviceId, STOPPED);
  }

  /**
   * Tests for program list calls
   */
  @Test
  public void testProgramList() throws Exception {
    // test initial state
    testListInitialState(TEST_NAMESPACE2, ProgramType.MAPREDUCE);
    testListInitialState(TEST_NAMESPACE1, ProgramType.WORKFLOW);
    testListInitialState(TEST_NAMESPACE2, ProgramType.SPARK);
    testListInitialState(TEST_NAMESPACE1, ProgramType.SERVICE);

    // deploy AllProgramsApp in namespace1 and verify
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);

    // deploy AppWithServices in namespace2 and verify
    deploy(AppWithServices.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);

    ApplicationSpecification allProgramSpec = Specifications.from(new AllProgramsApp());

    // verify list by namespace
    for (io.cdap.cdap.api.app.ProgramType type : io.cdap.cdap.api.app.ProgramType.values()) {
      Set<String> programsByType = allProgramSpec.getProgramsByType(type);
      verifyProgramList(TEST_NAMESPACE1, ProgramType.valueOf(type.name()), programsByType.size());
    }

    verifyProgramList(TEST_NAMESPACE2, ProgramType.SERVICE, 1);

    // verify list by app
    for (io.cdap.cdap.api.app.ProgramType type : io.cdap.cdap.api.app.ProgramType.values()) {
      Set<String> programsByType = allProgramSpec.getProgramsByType(type);
      verifyProgramList(TEST_NAMESPACE1, AllProgramsApp.NAME, ProgramType.valueOf(type.name()), programsByType.size());
    }

    verifyProgramList(TEST_NAMESPACE2, AppWithServices.NAME, ProgramType.SERVICE, 1);

    // verify invalid namespace
    Assert.assertEquals(404, getAppFDetailResponseCode(TEST_NAMESPACE1, AppWithServices.SERVICE_NAME));
    // verify invalid app
    Assert.assertEquals(404, getAppFDetailResponseCode(TEST_NAMESPACE1, "random"));
  }

  /**
   * Worker Specification tests
   */
  @Test
  public void testWorkerSpecification() throws Exception {
    // deploy AppWithWorker in namespace1 and verify
    deploy(AppWithWorker.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);

    verifyProgramSpecification(TEST_NAMESPACE1, AppWithWorker.NAME, ProgramType.WORKER.getCategoryName(),
                               AppWithWorker.WORKER);
    Assert.assertEquals(404, getProgramSpecificationResponseCode(TEST_NAMESPACE2, AppWithWorker.NAME,
                                                                 ProgramType.WORKER.getCategoryName(),
                                                                 AppWithWorker.WORKER));
  }

  @Test
  public void testServiceSpecification() throws Exception {
    deploy(AppWithServices.class, 200);
    HttpResponse response = doGet("/v3/namespaces/default/apps/AppWithServices/services/NoOpService");
    Assert.assertEquals(200, response.getResponseCode());

    Set<ServiceHttpEndpoint> expectedEndpoints = ImmutableSet.of(new ServiceHttpEndpoint("GET", "/ping"),
                                                                 new ServiceHttpEndpoint("POST", "/multi"),
                                                                 new ServiceHttpEndpoint("GET", "/multi"),
                                                                 new ServiceHttpEndpoint("GET", "/multi/ping"));

    GsonBuilder gsonBuilder = new GsonBuilder();
    gsonBuilder.registerTypeAdapter(ServiceSpecification.class, new ServiceSpecificationCodec());
    Gson gson = gsonBuilder.create();
    ServiceSpecification specification = readResponse(response, ServiceSpecification.class, gson);

    Set<ServiceHttpEndpoint> returnedEndpoints = new HashSet<>();
    for (HttpServiceHandlerSpecification httpServiceHandlerSpecification : specification.getHandlers().values()) {
      returnedEndpoints.addAll(httpServiceHandlerSpecification.getEndpoints());
    }

    Assert.assertEquals("NoOpService", specification.getName());
    Assert.assertEquals(returnedEndpoints, expectedEndpoints);
  }

  /**
   * Program specification tests through appfabric apis.
   */
  @Test
  public void testProgramSpecification() throws Exception {
    // deploy AllProgramsApp in namespace1 and verify
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);

    // deploy AppWithServices in namespace2 and verify
    deploy(AppWithServices.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);

    // deploy AppWithWorkflow in namespace2 and verify
    deploy(AppWithWorkflow.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);

    // deploy AppWithWorker in namespace1 and verify
    deploy(AppWithWorker.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);

    // verify program specification
    verifyProgramSpecification(TEST_NAMESPACE1, AllProgramsApp.NAME, ProgramType.SERVICE.getCategoryName(),
                               AllProgramsApp.NoOpService.NAME);
    verifyProgramSpecification(TEST_NAMESPACE1, AllProgramsApp.NAME, ProgramType.MAPREDUCE.getCategoryName(),
                               AllProgramsApp.NoOpMR.NAME);
    verifyProgramSpecification(TEST_NAMESPACE2, AppWithServices.NAME, ProgramType.SERVICE.getCategoryName(),
                               AppWithServices.SERVICE_NAME);
    verifyProgramSpecification(TEST_NAMESPACE2, AppWithWorkflow.NAME, ProgramType.WORKFLOW.getCategoryName(),
                               AppWithWorkflow.SampleWorkflow.NAME);
    verifyProgramSpecification(TEST_NAMESPACE1, AppWithWorker.NAME, ProgramType.WORKER.getCategoryName(),
                               AppWithWorker.WORKER);

    // verify invalid namespace
    Assert.assertEquals(404, getProgramSpecificationResponseCode(TEST_NAMESPACE1, AppWithServices.NAME,
                                                                 ProgramType.SERVICE.getCategoryName(),
                                                                 AppWithServices.SERVICE_NAME));
    // verify invalid app
    Assert.assertEquals(404, getProgramSpecificationResponseCode(TEST_NAMESPACE2, AppWithServices.NAME,
                                                                 ProgramType.WORKFLOW.getCategoryName(),
                                                                 AppWithWorkflow.SampleWorkflow.NAME));
    // verify invalid program type
    Assert.assertEquals(400, getProgramSpecificationResponseCode(TEST_NAMESPACE2, AppWithServices.NAME,
                                                                 "random", AppWithWorkflow.SampleWorkflow.NAME));

    // verify invalid program type
    Assert.assertEquals(404, getProgramSpecificationResponseCode(TEST_NAMESPACE2, AppWithWorker.NAME,
                                                                 ProgramType.WORKER.getCategoryName(),
                                                                 AppWithWorker.WORKER));
  }

  @Test
  public void testMultipleWorkflowSchedules() throws Exception {
    // Deploy the app
    NamespaceId testNamespace2 = new NamespaceId(TEST_NAMESPACE2);
    Id.Namespace idTestNamespace2 = Id.Namespace.fromEntityId(testNamespace2);
    Id.Artifact artifactId = Id.Artifact.from(idTestNamespace2, "appwithmultiplescheduledworkflows", VERSION1);
    addAppArtifact(artifactId, AppWithMultipleSchedules.class);
    AppRequest<? extends Config> appRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()));
    Id.Application appDefault = new Id.Application(idTestNamespace2, AppWithMultipleSchedules.NAME);
    ApplicationId app1 = testNamespace2.app(AppWithMultipleSchedules.NAME, VERSION1);
    ApplicationId app2 = testNamespace2.app(AppWithMultipleSchedules.NAME, VERSION2);
    Assert.assertEquals(200, deploy(appDefault, appRequest).getResponseCode());
    Assert.assertEquals(200, deploy(app1, appRequest).getResponseCode());
    Assert.assertEquals(200, deploy(app2, appRequest).getResponseCode());

    // Schedule details from non-versioned API
    List<ScheduleDetail> someSchedules = getSchedules(TEST_NAMESPACE2, AppWithMultipleSchedules.NAME,
                                                      AppWithMultipleSchedules.SOME_WORKFLOW);
    Assert.assertEquals(2, someSchedules.size());
    Assert.assertEquals(AppWithMultipleSchedules.SOME_WORKFLOW, someSchedules.get(0).getProgram().getProgramName());
    Assert.assertEquals(AppWithMultipleSchedules.SOME_WORKFLOW, someSchedules.get(1).getProgram().getProgramName());

    // Schedule details from non-versioned API
    List<ScheduleDetail> anotherSchedules = getSchedules(TEST_NAMESPACE2, AppWithMultipleSchedules.NAME,
                                                         AppWithMultipleSchedules.ANOTHER_WORKFLOW);
    Assert.assertEquals(3, anotherSchedules.size());
    Assert.assertEquals(AppWithMultipleSchedules.ANOTHER_WORKFLOW,
                        anotherSchedules.get(0).getProgram().getProgramName());
    Assert.assertEquals(AppWithMultipleSchedules.ANOTHER_WORKFLOW,
                        anotherSchedules.get(1).getProgram().getProgramName());
    Assert.assertEquals(AppWithMultipleSchedules.ANOTHER_WORKFLOW,
                        anotherSchedules.get(2).getProgram().getProgramName());

    // Schedule details from non-versioned API filtered by Trigger type
    List<ScheduleDetail> filteredTimeSchedules = getSchedules(TEST_NAMESPACE2, AppWithMultipleSchedules.NAME,
                                                              AppWithMultipleSchedules.TRIGGERED_WORKFLOW,
                                                              ProtoTrigger.Type.TIME);
    Assert.assertEquals(1, filteredTimeSchedules.size());
    assertProgramInSchedules(AppWithMultipleSchedules.TRIGGERED_WORKFLOW, filteredTimeSchedules);

    // Schedule details from non-versioned API filtered by Trigger type
    List<ScheduleDetail> programStatusSchedules = getSchedules(TEST_NAMESPACE2, AppWithMultipleSchedules.NAME,
                                                              AppWithMultipleSchedules.TRIGGERED_WORKFLOW,
                                                              ProtoTrigger.Type.PROGRAM_STATUS);
    Assert.assertEquals(4, programStatusSchedules.size());
    assertProgramInSchedules(AppWithMultipleSchedules.TRIGGERED_WORKFLOW, programStatusSchedules);

    deleteApp(appDefault, 200);

    // Schedule of app1 from versioned API
    List<ScheduleDetail> someSchedules1 = getSchedules(TEST_NAMESPACE2, AppWithMultipleSchedules.NAME, VERSION1,
                                                       AppWithMultipleSchedules.SOME_WORKFLOW);
    Assert.assertEquals(2, someSchedules1.size());
    assertProgramInSchedules(AppWithMultipleSchedules.SOME_WORKFLOW, someSchedules1);

    // Schedule details from versioned API filtered by Trigger type
    filteredTimeSchedules = getSchedules(TEST_NAMESPACE2, AppWithMultipleSchedules.NAME, VERSION1,
                                         AppWithMultipleSchedules.TRIGGERED_WORKFLOW,
                                         ProtoTrigger.Type.TIME);
    Assert.assertEquals(1, filteredTimeSchedules.size());
    assertProgramInSchedules(AppWithMultipleSchedules.TRIGGERED_WORKFLOW, filteredTimeSchedules);

    // Schedule details from versioned API filtered by Trigger type
    programStatusSchedules = getSchedules(TEST_NAMESPACE2, AppWithMultipleSchedules.NAME, VERSION1,
                                          AppWithMultipleSchedules.TRIGGERED_WORKFLOW,
                                          ProtoTrigger.Type.PROGRAM_STATUS);
    Assert.assertEquals(4, programStatusSchedules.size());
    assertProgramInSchedules(AppWithMultipleSchedules.TRIGGERED_WORKFLOW, programStatusSchedules);

    // Schedules triggered by SOME_WORKFLOW's completed or failed or killed status
    ProgramId someWorkflow = app1.workflow(AppWithMultipleSchedules.SOME_WORKFLOW);
    List<ScheduleDetail> triggeredSchedules1 = listSchedulesByTriggerProgram(TEST_NAMESPACE2, someWorkflow,
                                                                             ProgramStatus.COMPLETED,
                                                                             ProgramStatus.FAILED,
                                                                             ProgramStatus.KILLED);
    Assert.assertEquals(3, triggeredSchedules1.size());
    assertProgramInSchedules(AppWithMultipleSchedules.TRIGGERED_WORKFLOW, triggeredSchedules1);

    List<ScheduleDetail> filteredSchedules =
      listSchedulesByTriggerProgram(TEST_NAMESPACE2, someWorkflow, ProgramScheduleStatus.SCHEDULED,
                                    ProgramStatus.COMPLETED, ProgramStatus.FAILED, ProgramStatus.KILLED);
    // No schedule is enabled yet
    Assert.assertEquals(0, filteredSchedules.size());
    filteredSchedules = listSchedulesByTriggerProgram(TEST_NAMESPACE2, someWorkflow, ProgramScheduleStatus.SUSPENDED,
                                                      ProgramStatus.COMPLETED,
                                                      ProgramStatus.FAILED,
                                                      ProgramStatus.KILLED);
    // All schedules are suspended
    Assert.assertEquals(3, filteredSchedules.size());

    // Schedules triggered by SOME_WORKFLOW's completed status
    List<ScheduleDetail> triggeredByCompletedSchedules = listSchedulesByTriggerProgram(TEST_NAMESPACE2, someWorkflow,
                                                                                       ProgramStatus.COMPLETED);
    Assert.assertEquals(2, triggeredByCompletedSchedules.size());
    assertProgramInSchedules(AppWithMultipleSchedules.TRIGGERED_WORKFLOW, triggeredByCompletedSchedules);
    // Schedules triggered by ANOTHER_WORKFLOW regardless of program status
    ProgramId anotherWorkflow = app1.workflow(AppWithMultipleSchedules.ANOTHER_WORKFLOW);
    List<ScheduleDetail> triggeredSchedules2 = listSchedulesByTriggerProgram(TEST_NAMESPACE2, anotherWorkflow);
    Assert.assertEquals(1, triggeredSchedules2.size());
    assertProgramInSchedules(AppWithMultipleSchedules.TRIGGERED_WORKFLOW, triggeredSchedules2);

    deleteApp(app1, 200);

    // Schedule detail of app2 from versioned API
    List<ScheduleDetail> anotherSchedules2 = getSchedules(TEST_NAMESPACE2, AppWithMultipleSchedules.NAME,
                                                          VERSION2, AppWithMultipleSchedules.ANOTHER_WORKFLOW);
    Assert.assertEquals(3, anotherSchedules2.size());
    assertProgramInSchedules(AppWithMultipleSchedules.ANOTHER_WORKFLOW, anotherSchedules2);

    deleteApp(app2, 200);
  }

  private void assertProgramInSchedules(String programName, List<ScheduleDetail> schedules) {
    for (ScheduleDetail scheduleDetail : schedules) {
      Assert.assertEquals(programName, scheduleDetail.getProgram().getProgramName());
    }
  }

  @Test
  public void testServices() throws Exception {
    deploy(AppWithServices.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);

    Id.Service service1 = Id.Service.from(Id.Namespace.from(TEST_NAMESPACE1), AppWithServices.NAME,
                                          AppWithServices.SERVICE_NAME);
    final Id.Service service2 = Id.Service.from(Id.Namespace.from(TEST_NAMESPACE2), AppWithServices.NAME,
                                                AppWithServices.SERVICE_NAME);
    HttpResponse activeResponse = getServiceAvailability(service1);
    // Service is not valid, so it should return 404
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.code(), activeResponse.getResponseCode());

    activeResponse = getServiceAvailability(service2);
    // Service has not been started, so it should return 503
    Assert.assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
                        activeResponse.getResponseCode());

    // start service in wrong namespace
    startProgram(service1, 404);
    startProgram(service2);

    waitState(service2, RUNNING);

    Tasks.waitFor(200, () -> getServiceAvailability(service2).getResponseCode(),
                  2, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS);

    // verify instances
    try {
      getServiceInstances(service1);
      Assert.fail("Should not find service in " + TEST_NAMESPACE1);
    } catch (AssertionError expected) {
      // expected
    }
    ServiceInstances instances = getServiceInstances(service2);
    Assert.assertEquals(1, instances.getRequested());
    Assert.assertEquals(1, instances.getProvisioned());

    // request 2 additional instances
    int code = setServiceInstances(service1, 3);
    Assert.assertEquals(404, code);
    code = setServiceInstances(service2, 3);
    Assert.assertEquals(200, code);

    // verify that additional instances were provisioned
    instances = getServiceInstances(service2);
    Assert.assertEquals(3, instances.getRequested());
    Assert.assertEquals(3, instances.getProvisioned());

    // verify that endpoints are not available in the wrong namespace
    HttpResponse response = callService(service1, HttpMethod.POST, "multi");
    code = response.getResponseCode();
    Assert.assertEquals(404, code);

    response = callService(service1, HttpMethod.GET, "multi/ping");
    code = response.getResponseCode();
    Assert.assertEquals(404, code);

    // stop service
    stopProgram(service1, 404);
    stopProgram(service2);
    waitState(service2, STOPPED);

    activeResponse = getServiceAvailability(service2);
    // Service has been stopped, so it should return 503
    Assert.assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
                        activeResponse.getResponseCode());
  }

  @Test
  public void testSchedules() throws Exception {
    // deploy an app with schedule
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.fromEntityId(TEST_NAMESPACE_META1.getNamespaceId()),
                                              AppWithSchedule.NAME, VERSION1);
    addAppArtifact(artifactId, AppWithSchedule.class);
    AppRequest<? extends Config> request = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()));
    ApplicationId defaultAppId = TEST_NAMESPACE_META1.getNamespaceId().app(AppWithSchedule.NAME);
    Assert.assertEquals(200, deploy(defaultAppId, request).getResponseCode());

    // deploy another version of the app
    ApplicationId appV2Id = TEST_NAMESPACE_META1.getNamespaceId().app(AppWithSchedule.NAME, VERSION2);
    Assert.assertEquals(200, deploy(appV2Id, request).getResponseCode());

    // list schedules for default version app, for the workflow and for the app, they should be same
    List<ScheduleDetail> schedules =
      getSchedules(TEST_NAMESPACE1, AppWithSchedule.NAME, AppWithSchedule.WORKFLOW_NAME);
    Assert.assertEquals(1, schedules.size());
    ScheduleDetail schedule = schedules.get(0);
    Assert.assertEquals(SchedulableProgramType.WORKFLOW, schedule.getProgram().getProgramType());
    Assert.assertEquals(AppWithSchedule.WORKFLOW_NAME, schedule.getProgram().getProgramName());
    Assert.assertEquals(new TimeTrigger("0/15 * * * * ?"), schedule.getTrigger());

    // there should be two schedules now
    List<ScheduleDetail> schedulesForApp = listSchedules(TEST_NAMESPACE1, AppWithSchedule.NAME, null);
    Assert.assertEquals(1, schedulesForApp.size());
    Assert.assertEquals(schedules, schedulesForApp);

    List<ScheduleDetail> schedules2 =
      getSchedules(TEST_NAMESPACE1, AppWithSchedule.NAME, VERSION2, AppWithSchedule.WORKFLOW_NAME);
    Assert.assertEquals(1, schedules2.size());
    ScheduleDetail schedule2 = schedules2.get(0);
    Assert.assertEquals(SchedulableProgramType.WORKFLOW, schedule2.getProgram().getProgramType());
    Assert.assertEquals(AppWithSchedule.WORKFLOW_NAME, schedule2.getProgram().getProgramName());
    Assert.assertEquals(new TimeTrigger("0/15 * * * * ?"), schedule2.getTrigger());

    String newSchedule = "newTimeSchedule";
    testAddSchedule(newSchedule);
    testDeleteSchedule(appV2Id, newSchedule);
    testUpdateSchedule(appV2Id);
    testReEnableSchedule("reEnabledSchedule");
  }

  @Test
  public void testUpdateSchedulesFlag() throws Exception {
    // deploy an app with schedule
    AppWithSchedule.AppConfig config = new AppWithSchedule.AppConfig(true, true, true);

    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.fromEntityId(TEST_NAMESPACE_META2.getNamespaceId()),
                                              AppWithSchedule.NAME, VERSION1);
    addAppArtifact(artifactId, AppWithSchedule.class);
    AppRequest<? extends Config> request = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), config, null, null, false);

    ApplicationId defaultAppId = TEST_NAMESPACE_META2.getNamespaceId().app(AppWithSchedule.NAME);
    Assert.assertEquals(200, deploy(defaultAppId, request).getResponseCode());

    List<ScheduleDetail> actualSchedules = listSchedules(TEST_NAMESPACE_META2.getNamespaceId().getNamespace(),
                                                         defaultAppId.getApplication(), defaultAppId.getVersion());

    // none of the schedules will be added as we have set update schedules to be false
    Assert.assertEquals(0, actualSchedules.size());

    request = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), config, null, null, true);

    Assert.assertEquals(200, deploy(defaultAppId, request).getResponseCode());

    actualSchedules = listSchedules(TEST_NAMESPACE_META2.getNamespaceId().getNamespace(),
                                    defaultAppId.getApplication(), defaultAppId.getVersion());
    Assert.assertEquals(2, actualSchedules.size());

    // with workflow, without schedule
    config = new AppWithSchedule.AppConfig(true, false, false);
    request = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), config, null, null, false);
    Assert.assertEquals(200, deploy(defaultAppId, request).getResponseCode());

    // schedule should not be updated
    actualSchedules = listSchedules(TEST_NAMESPACE_META2.getNamespaceId().getNamespace(),
                                    defaultAppId.getApplication(),
                                    defaultAppId.getVersion());
    Assert.assertEquals(2, actualSchedules.size());

    // without workflow and schedule, schedule should be deleted
    config = new AppWithSchedule.AppConfig(false, false, false);
    request = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), config, null, null, false);
    Assert.assertEquals(200, deploy(defaultAppId, request).getResponseCode());

    actualSchedules = listSchedules(TEST_NAMESPACE_META2.getNamespaceId().getNamespace(),
                                   defaultAppId.getApplication(),
                                   defaultAppId.getVersion());
    Assert.assertEquals(0, actualSchedules.size());

    // with workflow and  one schedule, schedule should be added
    config = new AppWithSchedule.AppConfig(true, true, false);
    request = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), config, null, null, true);
    Assert.assertEquals(200, deploy(defaultAppId, request).getResponseCode());

    actualSchedules = listSchedules(TEST_NAMESPACE_META2.getNamespaceId().getNamespace(),
                                    defaultAppId.getApplication(),
                                    defaultAppId.getVersion());
    Assert.assertEquals(1, actualSchedules.size());
    Assert.assertEquals("SampleSchedule", actualSchedules.get(0).getName());

    // with workflow and two schedules, but update-schedules is false, so 2nd schedule should not get added
    config = new AppWithSchedule.AppConfig(true, true, true);
    request = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), config, null, null, false);
    Assert.assertEquals(200, deploy(defaultAppId, request).getResponseCode());

    actualSchedules = listSchedules(TEST_NAMESPACE_META2.getNamespaceId().getNamespace(),
                                    defaultAppId.getApplication(),
                                    defaultAppId.getVersion());
    Assert.assertEquals(1, actualSchedules.size());
    Assert.assertEquals("SampleSchedule", actualSchedules.get(0).getName());

    // same config, but update-schedule flag is true now, so 2 schedules should be available now
    request = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), config, null, null, true);
    Assert.assertEquals(200, deploy(defaultAppId, request).getResponseCode());

    actualSchedules = listSchedules(TEST_NAMESPACE_META2.getNamespaceId().getNamespace(),
                                   defaultAppId.getApplication(),
                                   defaultAppId.getVersion());
    Assert.assertEquals(2, actualSchedules.size());
  }

  @Test
  public void testStartProgramWithDisabledProfile() throws Exception {
    // put my profile and disable it, using this profile to start program should fail
    ProfileId profileId = new NamespaceId(TEST_NAMESPACE1).profile("MyProfile");
    Profile profile = new Profile("MyProfile", Profile.NATIVE.getLabel(), Profile.NATIVE.getDescription(),
                                  Profile.NATIVE.getScope(), Profile.NATIVE.getProvisioner());
    putProfile(profileId, profile, 200);
    disableProfile(profileId, 200);

    // deploy, check the status
    deploy(AppWithWorkflow.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);

    ProgramId programId =
      new NamespaceId(TEST_NAMESPACE1).app(AppWithWorkflow.NAME).workflow(AppWithWorkflow.SampleWorkflow.NAME);

    // workflow is stopped initially
    Assert.assertEquals(STOPPED, getProgramStatus(programId));

    // start workflow should give a 409 since we have a runtime argument associated with a disabled profile
    startProgram(programId, Collections.singletonMap(SystemArguments.PROFILE_NAME, profileId.getScopedName()), 409);
    Assert.assertEquals(STOPPED, getProgramStatus(programId));

    // use native profile to start workflow should work since it is always enabled.
    // the workflow should start but fail because we are not passing in required runtime args.
    int runs = getProgramRuns(programId, ProgramRunStatus.FAILED).size();

    startProgram(programId, Collections.singletonMap(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName()),
                 200);

    // wait for the workflow to stop and check the status
    Tasks.waitFor(runs + 1, () -> getProgramRuns(programId, ProgramRunStatus.FAILED).size(), 60, TimeUnit.SECONDS);
  }

  private void testReEnableSchedule(String scheduleName) throws Exception {
    ProtoTrigger.TimeTrigger protoTime = new ProtoTrigger.TimeTrigger("0 * * * ?");
    ProtoTrigger.PartitionTrigger protoPartition =
      new ProtoTrigger.PartitionTrigger(NamespaceId.DEFAULT.dataset("data"), 5);
    String description = "Something";
    ScheduleProgramInfo programInfo = new ScheduleProgramInfo(SchedulableProgramType.WORKFLOW,
                                                              AppWithSchedule.WORKFLOW_NAME);
    ImmutableMap<String, String> properties = ImmutableMap.of("a", "b", "c", "d");
    TimeTrigger timeTrigger = new TimeTrigger("0 * * * ?");
    ScheduleDetail timeDetail = new ScheduleDetail(TEST_NAMESPACE1, AppWithSchedule.NAME, ApplicationId.DEFAULT_VERSION,
                                                   scheduleName, description, programInfo, properties,
                                                   timeTrigger, Collections.emptyList(),
                                                   Schedulers.JOB_QUEUE_TIMEOUT_MILLIS, null, null);
    HttpResponse response = addSchedule(TEST_NAMESPACE1, AppWithSchedule.NAME, null, scheduleName, timeDetail);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());

    // start schedule
    Assert.assertEquals(HttpResponseStatus.OK.code(),
                        resumeSchedule(TEST_NAMESPACE1, AppWithSchedule.NAME, scheduleName));

    // suspend schedule
    long startTime = System.currentTimeMillis();
    Assert.assertEquals(HttpResponseStatus.OK.code(),
                        suspendSchedule(TEST_NAMESPACE1, AppWithSchedule.NAME, scheduleName));
    long endTime = System.currentTimeMillis() + 1;

    // re-enable schedule
    Assert.assertEquals(HttpResponseStatus.OK.code(),
                        reEnableSchedules(TEST_NAMESPACE1, startTime, endTime));

    // assert schedule is running by suspending again
    Assert.assertEquals(HttpResponseStatus.OK.code(),
                        suspendSchedule(TEST_NAMESPACE1, AppWithSchedule.NAME, scheduleName));
  }

  private void testAddSchedule(String scheduleName) throws Exception {
    String partitionScheduleName = scheduleName + "Partition";
    String orScheduleName = scheduleName + "Or";
    ProtoTrigger.TimeTrigger protoTime = new ProtoTrigger.TimeTrigger("0 * * * ?");
    ProtoTrigger.PartitionTrigger protoPartition =
      new ProtoTrigger.PartitionTrigger(NamespaceId.DEFAULT.dataset("data"), 5);
    ProtoTrigger.OrTrigger protoOr = ProtoTrigger.or(protoTime, protoPartition);
    String description = "Something";
    ScheduleProgramInfo programInfo = new ScheduleProgramInfo(SchedulableProgramType.WORKFLOW,
                                                              AppWithSchedule.WORKFLOW_NAME);
    ImmutableMap<String, String> properties = ImmutableMap.of("a", "b", "c", "d");
    TimeTrigger timeTrigger = new TimeTrigger("0 * * * ?");
    ScheduleDetail timeDetail = new ScheduleDetail(TEST_NAMESPACE1, AppWithSchedule.NAME, ApplicationId.DEFAULT_VERSION,
                                                   scheduleName, description, programInfo, properties,
                                                   timeTrigger, Collections.emptyList(),
                                                   Schedulers.JOB_QUEUE_TIMEOUT_MILLIS, null, null);
    PartitionTrigger partitionTrigger =
      new PartitionTrigger(protoPartition.getDataset(), protoPartition.getNumPartitions());
    ScheduleDetail expectedPartitionDetail =
      new ScheduleDetail(TEST_NAMESPACE1, AppWithSchedule.NAME, ApplicationId.DEFAULT_VERSION,
                         partitionScheduleName, description, programInfo, properties, partitionTrigger,
                         Collections.emptyList(), Schedulers.JOB_QUEUE_TIMEOUT_MILLIS, null, null);

    ScheduleDetail requestPartitionDetail =
      new ScheduleDetail(TEST_NAMESPACE1, AppWithSchedule.NAME, ApplicationId.DEFAULT_VERSION,
                         partitionScheduleName, description, programInfo, properties, protoPartition,
                         Collections.emptyList(), Schedulers.JOB_QUEUE_TIMEOUT_MILLIS, null, null);

    ScheduleDetail expectedOrDetail =
      new ScheduleDetail(TEST_NAMESPACE1, AppWithSchedule.NAME, ApplicationId.DEFAULT_VERSION,
                         orScheduleName, description, programInfo, properties,
                         new OrTrigger(timeTrigger, partitionTrigger),
                         Collections.emptyList(), Schedulers.JOB_QUEUE_TIMEOUT_MILLIS, null, null);

    ScheduleDetail requestOrDetail =
      new ScheduleDetail(TEST_NAMESPACE1, AppWithSchedule.NAME, ApplicationId.DEFAULT_VERSION,
                         orScheduleName, description, programInfo, properties, protoOr,
                         Collections.emptyList(), Schedulers.JOB_QUEUE_TIMEOUT_MILLIS, null, null);

    // trying to add the schedule with different name in path param than schedule spec should fail
    HttpResponse response = addSchedule(TEST_NAMESPACE1, AppWithSchedule.NAME, null, "differentName", timeDetail);
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.getResponseCode());

    // adding a schedule to a non-existing app should fail
    response = addSchedule(TEST_NAMESPACE1, "nonExistingApp", null, scheduleName, timeDetail);
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.getResponseCode());

    // adding a schedule to invalid type of program type should fail
    ScheduleDetail invalidScheduleDetail = new ScheduleDetail(
      scheduleName, "Something", new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE, AppWithSchedule.MAPREDUCE),
      properties, protoTime, Collections.emptyList(), TimeUnit.MINUTES.toMillis(1));
    response = addSchedule(TEST_NAMESPACE1, AppWithSchedule.NAME, null, scheduleName, invalidScheduleDetail);
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.getResponseCode());

    // adding a schedule for a program that does not exist
    ScheduleDetail nonExistingDetail =
      new ScheduleDetail(TEST_NAMESPACE1, AppWithSchedule.NAME, ApplicationId.DEFAULT_VERSION,
                         scheduleName, description, new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE, "nope"),
                         properties, timeTrigger, Collections.emptyList(),
                         Schedulers.JOB_QUEUE_TIMEOUT_MILLIS, null, null);
    response = addSchedule(TEST_NAMESPACE1, AppWithSchedule.NAME, null, scheduleName, nonExistingDetail);
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.getResponseCode());

    // test adding a schedule
    response = addSchedule(TEST_NAMESPACE1, AppWithSchedule.NAME, null, scheduleName, timeDetail);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());

    response = addSchedule(TEST_NAMESPACE1, AppWithSchedule.NAME, null, partitionScheduleName, requestPartitionDetail);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());

    response = addSchedule(TEST_NAMESPACE1, AppWithSchedule.NAME, null, orScheduleName, requestOrDetail);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());

    List<ScheduleDetail> schedules = getSchedules(TEST_NAMESPACE1, AppWithSchedule.NAME, AppWithSchedule.WORKFLOW_NAME);
    Assert.assertEquals(4, schedules.size());
    Assert.assertEquals(timeDetail, schedules.get(1));
    Assert.assertEquals(expectedOrDetail, schedules.get(2));
    Assert.assertEquals(expectedPartitionDetail, schedules.get(3));

    List<ScheduleDetail> schedulesForApp = listSchedules(TEST_NAMESPACE1, AppWithSchedule.NAME, null);
    Assert.assertEquals(schedules, schedulesForApp);

    // trying to add ScheduleDetail of the same schedule again should fail with AlreadyExistsException
    response = addSchedule(TEST_NAMESPACE1, AppWithSchedule.NAME, null, scheduleName, timeDetail);
    Assert.assertEquals(HttpResponseStatus.CONFLICT.code(), response.getResponseCode());

    // although we should be able to add schedule to a different version of the app
    response = addSchedule(TEST_NAMESPACE1, AppWithSchedule.NAME, VERSION2, scheduleName, timeDetail);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());

    // this should not have affected the schedules of the default version
    List<ScheduleDetail> scheds = getSchedules(TEST_NAMESPACE1, AppWithSchedule.NAME, AppWithSchedule.WORKFLOW_NAME);
    Assert.assertEquals(schedules, scheds);

    // there should be two schedules now for version 2
    List<ScheduleDetail> schedules2 =
      getSchedules(TEST_NAMESPACE1, AppWithSchedule.NAME, VERSION2, AppWithSchedule.WORKFLOW_NAME);
    Assert.assertEquals(2, schedules2.size());
    Assert.assertEquals(timeDetail, schedules2.get(1));

    List<ScheduleDetail> schedulesForApp2 = listSchedules(TEST_NAMESPACE1, AppWithSchedule.NAME, VERSION2);
    Assert.assertEquals(schedules2, schedulesForApp2);

    // Add a schedule with no schedule name in spec
    ScheduleDetail detail2 = new ScheduleDetail(TEST_NAMESPACE1, AppWithSchedule.NAME, VERSION2,
                                                null, "Something 2", programInfo, properties,
                                                new TimeTrigger("0 * * * ?"),
                                                Collections.emptyList(), TimeUnit.HOURS.toMillis(6), null, null);
    response = addSchedule(TEST_NAMESPACE1, AppWithSchedule.NAME, VERSION2, "schedule-100", detail2);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
    ScheduleDetail detail100 = getSchedule(TEST_NAMESPACE1, AppWithSchedule.NAME, VERSION2, "schedule-100");
    Assert.assertEquals("schedule-100", detail100.getName());
    Assert.assertEquals(detail2.getTimeoutMillis(), detail100.getTimeoutMillis());
  }

  private void testDeleteSchedule(ApplicationId appV2Id, String scheduleName) throws Exception {
    // trying to delete a schedule from a non-existing app should fail
    HttpResponse response = deleteSchedule(TEST_NAMESPACE1, "nonExistingApp", null, scheduleName);
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.getResponseCode());

    // trying to delete a non-existing schedule should fail
    response = deleteSchedule(TEST_NAMESPACE1, AppWithSchedule.NAME, null, "nonExistingSchedule");
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.getResponseCode());

    // trying to delete a valid existing schedule should pass
    response = deleteSchedule(TEST_NAMESPACE1, AppWithSchedule.NAME, null, scheduleName);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
    List<ScheduleDetail> schedules = getSchedules(TEST_NAMESPACE1, AppWithSchedule.NAME, AppWithSchedule.WORKFLOW_NAME);
    Assert.assertEquals(3, schedules.size());

    // the above schedule delete should not have affected the schedule with same name in another version of the app
    schedules = getSchedules(TEST_NAMESPACE1, AppWithSchedule.NAME, appV2Id.getVersion(),
                             AppWithSchedule.WORKFLOW_NAME);
    Assert.assertEquals(3, schedules.size());

    // should have a schedule with the given name
    boolean foundSchedule = false;
    for (ScheduleDetail schedule : schedules) {
      if (schedule.getName().equals(scheduleName)) {
        foundSchedule = true;
      }
    }
    Assert.assertTrue(String.format("Expected to find a schedule named %s but didn't", scheduleName), foundSchedule);

    // delete the schedule from the other version of the app too as a cleanup
    response = deleteSchedule(TEST_NAMESPACE1, AppWithSchedule.NAME, appV2Id.getVersion(), scheduleName);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
    schedules = getSchedules(TEST_NAMESPACE1, AppWithSchedule.NAME, appV2Id.getVersion(),
                             AppWithSchedule.WORKFLOW_NAME);
    Assert.assertEquals(2, schedules.size());
  }

  private void testUpdateSchedule(ApplicationId appV2Id) throws Exception {
    ScheduleDetail updateDetail = new ScheduleDetail(AppWithSchedule.SCHEDULE, "updatedDescription", null,
                                                     ImmutableMap.of("twoKey", "twoValue", "someKey", "newValue"),
                                                     new TimeTrigger("0 4 * * *"),
                                                     ImmutableList.of(new ConcurrencyConstraint(5)), null);

    // trying to update schedule for a non-existing app should fail
    HttpResponse response = updateSchedule(TEST_NAMESPACE1, "nonExistingApp", null, AppWithSchedule.SCHEDULE,
                                           updateDetail);
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.getResponseCode());

    // trying to update a non-existing schedule should fail
    ScheduleDetail nonExistingSchedule = new ScheduleDetail("NonExistingSchedule", "updatedDescription", null,
                                                            ImmutableMap.of("twoKey", "twoValue"),
                                                            new TimeTrigger("0 4 * * *"),
                                                            ImmutableList.of(new ConcurrencyConstraint(5)), null);
    response = updateSchedule(TEST_NAMESPACE1, AppWithSchedule.NAME, null,
                              "NonExistingSchedule", nonExistingSchedule);
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.getResponseCode());

    // should be able to update an existing schedule with a valid new time schedule
    response = updateSchedule(TEST_NAMESPACE1, AppWithSchedule.NAME, null, AppWithSchedule.SCHEDULE,
                              updateDetail);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());

    // verify that the schedule information for updated
    ScheduleDetail schedule = getSchedule(TEST_NAMESPACE1, AppWithSchedule.NAME, null, AppWithSchedule.SCHEDULE);
    Assert.assertEquals("updatedDescription", schedule.getDescription());
    Assert.assertEquals("0 4 * * *", ((TimeTrigger) schedule.getTrigger()).getCronExpression());
    Assert.assertEquals(new ProtoConstraint.ConcurrencyConstraint(5), schedule.getConstraints().get(0));
    // the properties should have been replaced
    Assert.assertEquals(2, schedule.getProperties().size());
    Assert.assertEquals("newValue", schedule.getProperties().get("someKey"));
    Assert.assertEquals("twoValue", schedule.getProperties().get("twoKey"));
    // the old property should not exist
    Assert.assertNull(schedule.getProperties().get("oneKey"));

    // the above update should not have affected the schedule for the other version of the app
    schedule = getSchedule(TEST_NAMESPACE1, AppWithSchedule.NAME, appV2Id.getVersion(), AppWithSchedule.SCHEDULE);
    Assert.assertNotEquals("updatedDescription", schedule.getDescription());
    Assert.assertEquals("0/15 * * * * ?", ((TimeTrigger) schedule.getTrigger()).getCronExpression());

    // try to update the schedule again but this time with property as null. It should retain the old properties
    ScheduleDetail scheduleDetail = new ScheduleDetail(AppWithSchedule.SCHEDULE, "updatedDescription", null, null,
                                                       new ProtoTrigger.TimeTrigger("0 4 * * *"), null, null);
    response = updateSchedule(TEST_NAMESPACE1, AppWithSchedule.NAME, null, AppWithSchedule.SCHEDULE, scheduleDetail);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
    schedule = getSchedule(TEST_NAMESPACE1, AppWithSchedule.NAME, null, AppWithSchedule.SCHEDULE);
    Assert.assertEquals(2, schedule.getProperties().size());
    Assert.assertEquals("newValue", schedule.getProperties().get("someKey"));
    Assert.assertEquals("twoValue", schedule.getProperties().get("twoKey"));
    Assert.assertEquals(new ProtoConstraint.ConcurrencyConstraint(5), schedule.getConstraints().get(0));
  }

  @After
  public void cleanup() throws Exception {
    doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2));
  }

  private HttpResponse getServiceAvailability(Id.Service serviceId) throws Exception {
    String activeUrl = String.format("apps/%s/services/%s/available", serviceId.getApplicationId(), serviceId.getId());
    String versionedActiveUrl = getVersionedAPIPath(activeUrl, Constants.Gateway.API_VERSION_3_TOKEN,
                                                    serviceId.getNamespaceId());
    return doGet(versionedActiveUrl);
  }

  private ServiceInstances getServiceInstances(Id.Service serviceId) throws Exception {
    String instanceUrl = String.format("apps/%s/services/%s/instances", serviceId.getApplicationId(),
                                       serviceId.getId());
    String versionedInstanceUrl = getVersionedAPIPath(instanceUrl, Constants.Gateway.API_VERSION_3_TOKEN,
                                                      serviceId.getNamespaceId());
    HttpResponse response = doGet(versionedInstanceUrl);
    Assert.assertEquals(200, response.getResponseCode());
    return readResponse(response, ServiceInstances.class);
  }

  private int setServiceInstances(Id.Service serviceId, int instances) throws Exception {
    String instanceUrl = String.format("apps/%s/services/%s/instances", serviceId.getApplicationId(),
                                       serviceId.getId());
    String versionedInstanceUrl = getVersionedAPIPath(instanceUrl, Constants.Gateway.API_VERSION_3_TOKEN,
                                                      serviceId.getNamespaceId());
    String instancesBody = GSON.toJson(new Instances(instances));
    return doPut(versionedInstanceUrl, instancesBody).getResponseCode();
  }

  private HttpResponse callService(Id.Service serviceId, HttpMethod method, String endpoint) throws Exception {
    String serviceUrl = String.format("apps/%s/service/%s/methods/%s",
                                      serviceId.getApplicationId(), serviceId.getId(), endpoint);
    String versionedServiceUrl = getVersionedAPIPath(serviceUrl, Constants.Gateway.API_VERSION_3_TOKEN,
                                                     serviceId.getNamespaceId());
    if (HttpMethod.GET.equals(method)) {
      return doGet(versionedServiceUrl);
    } else if (HttpMethod.POST.equals(method)) {
      return doPost(versionedServiceUrl);
    }
    throw new IllegalArgumentException("Only GET and POST supported right now.");
  }

  private void verifyProgramSpecification(String namespace, String appId, String programType, String programId)
    throws Exception {
    JsonObject programSpec = getProgramSpecification(namespace, appId, programType, programId);
    Assert.assertTrue(programSpec.has("className") && programSpec.has("name") && programSpec.has("description"));
    Assert.assertEquals(programId, programSpec.get("name").getAsString());
  }

  private JsonObject getProgramSpecification(String namespace, String appId, String programType,
                                             String programId) throws Exception {
    HttpResponse response = requestProgramSpecification(namespace, appId, programType, programId);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), JsonObject.class);
  }

  private int getProgramSpecificationResponseCode(String namespace, String appId, String programType, String programId)
    throws Exception {
    HttpResponse response = requestProgramSpecification(namespace, appId, programType, programId);
    return response.getResponseCode();
  }

  private HttpResponse requestProgramSpecification(String namespace, String appId, String programType,
                                                   String programId) throws Exception {
    String uri = getVersionedAPIPath(String.format("apps/%s/%s/%s", appId, programType, programId),
                                     Constants.Gateway.API_VERSION_3_TOKEN, namespace);
    return doGet(uri);
  }

  private void testListInitialState(String namespace, ProgramType programType) throws Exception {
    HttpResponse response = doGet(getVersionedAPIPath(programType.getCategoryName(),
                                                      Constants.Gateway.API_VERSION_3_TOKEN, namespace));
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals(EMPTY_ARRAY_JSON, response.getResponseBodyAsString());
  }

  private void verifyProgramList(String namespace, ProgramType programType, int expected) throws Exception {
    HttpResponse response = requestProgramList(namespace, programType.getCategoryName());
    Assert.assertEquals(200, response.getResponseCode());
    List<Map<String, String>> programs = GSON.fromJson(response.getResponseBodyAsString(), LIST_MAP_STRING_STRING_TYPE);
    Assert.assertEquals(expected, programs.size());
  }

  private void verifyProgramList(String namespace, String appName,
                                 final ProgramType programType, int expected) throws Exception {
    HttpResponse response = requestAppDetail(namespace, appName);
    Assert.assertEquals(200, response.getResponseCode());
    ApplicationDetail appDetail = GSON.fromJson(response.getResponseBodyAsString(), ApplicationDetail.class);
    Collection<ProgramRecord> programs = Collections2.filter(
      appDetail.getPrograms(), record -> programType.getCategoryName().equals(record.getType().getCategoryName()));
    Assert.assertEquals(expected, programs.size());
  }

  private int getAppFDetailResponseCode(String namespace, @Nullable String appName) throws Exception {
    HttpResponse response = requestAppDetail(namespace, appName);
    return response.getResponseCode();
  }

  private HttpResponse requestProgramList(String namespace, String programType)
    throws Exception {
    return doGet(getVersionedAPIPath(programType, Constants.Gateway.API_VERSION_3_TOKEN, namespace));
  }

  private HttpResponse requestAppDetail(String namespace, String appName)
    throws Exception {
    String uri = getVersionedAPIPath(String.format("apps/%s", appName),
                                     Constants.Gateway.API_VERSION_3_TOKEN, namespace);
    return doGet(uri);
  }

  private void verifyInitialBatchStatusOutput(HttpResponse response) {
    Assert.assertEquals(200, response.getResponseCode());
    List<JsonObject> returnedBody = readResponse(response, LIST_OF_JSONOBJECT_TYPE);
    for (JsonObject obj : returnedBody) {
      Assert.assertEquals(200, obj.get("statusCode").getAsInt());
      Assert.assertEquals(STOPPED, obj.get("status").getAsString());
    }
  }

  private void verifyInitialBatchInstanceOutput(HttpResponse response) {
    Assert.assertEquals(200, response.getResponseCode());
    List<JsonObject> returnedBody = readResponse(response, LIST_OF_JSONOBJECT_TYPE);
    for (JsonObject obj : returnedBody) {
      Assert.assertEquals(200, obj.get("statusCode").getAsInt());
      Assert.assertEquals(1, obj.get("requested").getAsInt());
      Assert.assertEquals(0, obj.get("provisioned").getAsInt());
    }
  }

  private void testHistory(Class<?> app, Id.Program program) throws Exception {
    String namespace = program.getNamespaceId();
    deploy(app, 200, Constants.Gateway.API_VERSION_3_TOKEN, namespace);
    verifyProgramHistory(program.toEntityId());
    deleteApp(program.getApplication(), 200);

    ApplicationId appId = new ApplicationId(namespace, program.getApplicationId(), VERSION1);
    ProgramId programId = appId.program(program.getType(), program.getId());
    Id.Artifact artifactId = Id.Artifact.from(program.getNamespace(), app.getSimpleName(), "1.0.0");
    addAppArtifact(artifactId, app);
    AppRequest<Config> request = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), null);
    Assert.assertEquals(200, deploy(appId, request).getResponseCode());
    verifyProgramHistory(programId);
    deleteApp(appId, 200);
  }

  private void verifyProgramHistory(ProgramId program) throws Exception {
    // first run
    startProgram(program, 200);
    waitState(program, RUNNING);

    stopProgram(program, null, 200, null);
    waitState(program, STOPPED);

    // second run
    startProgram(program, 200);
    waitState(program, RUNNING);

    // one run should be active
    historyStatusWithRetry(program, ProgramRunStatus.RUNNING, 1);

    historyStatusWithRetry(program, ProgramRunStatus.KILLED, 1);

    // stop the second run
    stopProgram(program, null, 200, null);
    waitState(program, STOPPED);

    historyStatusWithRetry(program, ProgramRunStatus.KILLED, 2);
  }

  private void historyStatusWithRetry(ProgramId program, ProgramRunStatus status, int size) throws Exception {
    String urlAppVersionPart = ApplicationId.DEFAULT_VERSION.equals(program.getVersion()) ?
      "" : "/versions/" + program.getVersion();
    String basePath = String.format("apps/%s%s/%s/%s/runs", program.getApplication(), urlAppVersionPart,
                                    program.getType().getCategoryName(), program.getProgram());
    String runsUrl = getVersionedAPIPath(basePath + "?status=" + status.name(),
                                         Constants.Gateway.API_VERSION_3_TOKEN, program.getNamespace());
    int trials = 0;
    while (trials++ < 5) {
      HttpResponse response = doGet(runsUrl);
      List<RunRecord> result = GSON.fromJson(response.getResponseBodyAsString(), LIST_OF_RUN_RECORD);
      if (result != null && result.size() >= size) {
        for (RunRecord m : result) {
          String runUrl = getVersionedAPIPath(basePath + "/" + m.getPid(), Constants.Gateway.API_VERSION_3_TOKEN,
                                              program.getNamespace());
          response = doGet(runUrl);
          RunRecord actualRunRecord = GSON.fromJson(response.getResponseBodyAsString(), RunRecord.class);
          Assert.assertEquals(m.getStatus(), actualRunRecord.getStatus());
        }
        break;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertTrue(trials < 5);
  }

  private void testVersionedProgramRuntimeArgs(ProgramId programId) throws Exception {
    String versionedRuntimeArgsUrl = getVersionedAPIPath("apps/" + programId.getApplication()
                                                           + "/versions/" + programId.getVersion()
                                                           + "/" + programId.getType().getCategoryName()
                                                           + "/" + programId.getProgram() + "/runtimeargs",
                                                         Constants.Gateway.API_VERSION_3_TOKEN,
                                                         programId.getNamespace());
    verifyRuntimeArgs(versionedRuntimeArgsUrl);
  }

  private void testRuntimeArgs(Class<?> app, String namespace, String appId, String programType, String programId)
    throws Exception {
    deploy(app, 200, Constants.Gateway.API_VERSION_3_TOKEN, namespace);

    String versionedRuntimeArgsUrl = getVersionedAPIPath("apps/" + appId + "/" + programType + "/" + programId +
                                                           "/runtimeargs", Constants.Gateway.API_VERSION_3_TOKEN,
                                                         namespace);
    verifyRuntimeArgs(versionedRuntimeArgsUrl);

    String versionedRuntimeArgsAppVersionUrl = getVersionedAPIPath("apps/" + appId
                                                                     + "/versions/" + ApplicationId.DEFAULT_VERSION
                                                                     + "/" + programType
                                                                     + "/" + programId + "/runtimeargs",
                                                                   Constants.Gateway.API_VERSION_3_TOKEN, namespace);
    verifyRuntimeArgs(versionedRuntimeArgsAppVersionUrl);
  }

  private void verifyRuntimeArgs(String url) throws Exception {
    Map<String, String> args = Maps.newHashMap();
    args.put("Key1", "Val1");
    args.put("Key2", "Val2");
    args.put("Key3", "Val3");

    HttpResponse response;
    Type mapStringStringType = new TypeToken<Map<String, String>>() { }.getType();

    String argString = GSON.toJson(args, mapStringStringType);

    response = doPut(url, argString);

    Assert.assertEquals(200, response.getResponseCode());
    response = doGet(url);

    Assert.assertEquals(200, response.getResponseCode());
    Map<String, String> argsRead = GSON.fromJson(response.getResponseBodyAsString(), mapStringStringType);

    Assert.assertEquals(args.size(), argsRead.size());

    for (Map.Entry<String, String> entry : args.entrySet()) {
      Assert.assertEquals(entry.getValue(), argsRead.get(entry.getKey()));
    }

    //test empty runtime args
    response = doPut(url, "");
    Assert.assertEquals(200, response.getResponseCode());

    response = doGet(url);
    Assert.assertEquals(200, response.getResponseCode());
    argsRead = GSON.fromJson(response.getResponseBodyAsString(), mapStringStringType);
    Assert.assertEquals(0, argsRead.size());

    //test null runtime args
    response = doPut(url, null);
    Assert.assertEquals(200, response.getResponseCode());

    response = doGet(url);
    Assert.assertEquals(200, response.getResponseCode());
    argsRead = GSON.fromJson(response.getResponseBodyAsString(), mapStringStringType);
    Assert.assertEquals(0, argsRead.size());
  }
}
