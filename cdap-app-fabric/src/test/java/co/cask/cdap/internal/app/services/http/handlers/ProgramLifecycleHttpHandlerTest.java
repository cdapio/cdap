/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.AppWithMultipleScheduledWorkflows;
import co.cask.cdap.AppWithSchedule;
import co.cask.cdap.AppWithServices;
import co.cask.cdap.AppWithStreamSizeSchedule;
import co.cask.cdap.AppWithWorker;
import co.cask.cdap.AppWithWorkflow;
import co.cask.cdap.ConcurrentWorkflowApp;
import co.cask.cdap.DummyAppWithTrackingTable;
import co.cask.cdap.MultiStreamApp;
import co.cask.cdap.SleepingWorkflowApp;
import co.cask.cdap.WordCountApp;
import co.cask.cdap.WorkflowAppWithErrorRuns;
import co.cask.cdap.WorkflowAppWithScopedParameters;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.queue.DequeueStrategy;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.queue.QueueConsumer;
import co.cask.cdap.data2.queue.QueueEntry;
import co.cask.cdap.data2.queue.QueueProducer;
import co.cask.cdap.gateway.handlers.ProgramLifecycleHttpHandler;
import co.cask.cdap.internal.app.ScheduleSpecificationCodec;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.Instances;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ServiceInstances;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.XSlowTests;
import co.cask.common.http.HttpMethod;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.After;
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
import javax.annotation.Nullable;

/**
 * Tests for {@link ProgramLifecycleHttpHandler}
 */
public class ProgramLifecycleHttpHandlerTest extends AppFabricTestBase {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(ScheduleSpecification.class, new ScheduleSpecificationCodec())
    .create();
  private static final Type LIST_OF_JSONOBJECT_TYPE = new TypeToken<List<JsonObject>>() { }.getType();

  private static final String WORDCOUNT_APP_NAME = "WordCountApp";
  private static final String WORDCOUNT_FLOW_NAME = "WordCountFlow";
  private static final String WORDCOUNT_MAPREDUCE_NAME = "VoidMapReduceJob";
  private static final String WORDCOUNT_FLOWLET_NAME = "StreamSource";
  private static final String DUMMY_APP_ID = "dummy";
  private static final String DUMMY_RUNNABLE_ID = "dummy-batch";
  private static final String SLEEP_WORKFLOW_APP_ID = "SleepWorkflowApp";
  private static final String SLEEP_WORKFLOW_RUNNABLE_ID = "SleepWorkflow";
  private static final String APP_WITH_SERVICES_APP_ID = "AppWithServices";
  private static final String APP_WITH_SERVICES_SERVICE_NAME = "NoOpService";
  private static final String APP_WITH_WORKFLOW_APP_ID = "AppWithWorkflow";
  private static final String APP_WITH_WORKFLOW_WORKFLOW_NAME = "SampleWorkflow";
  private static final String APP_WITH_STREAM_SCHEDULE_APP_NAME = "AppWithStreamSizeSchedule";
  private static final String APP_WITH_STREAM_SCHEDULE_WORKFLOW_NAME = "SampleWorkflow";
  private static final String APP_WITH_STREAM_SCHEDULE_STREAM_NAME = "stream";
  private static final String APP_WITH_SCHEDULE_APP_NAME = "AppWithSchedule";
  private static final String APP_WITH_SCHEDULE_WORKFLOW_NAME = "SampleWorkflow";
  private static final String APP_WITH_MULTIPLE_WORKFLOWS_APP_NAME = "AppWithMultipleScheduledWorkflows";
  private static final String APP_WITH_MULTIPLE_WORKFLOWS_SOMEWORKFLOW = "SomeWorkflow";
  private static final String APP_WITH_MULTIPLE_WORKFLOWS_ANOTHERWORKFLOW = "AnotherWorkflow";
  private static final String APP_WITH_CONCURRENT_WORKFLOW = "ConcurrentWorkflowApp";
  private static final String CONCURRENT_WORKFLOW_NAME = "ConcurrentWorkflow";
  private static final String WORKFLOW_APP_WITH_ERROR_RUNS = "WorkflowAppWithErrorRuns";
  private static final String WORKFLOW_WITH_ERROR_RUNS = "WorkflowWithErrorRuns";
  private static final String WORKFLOW_APP_WITH_SCOPED_PARAMETERS = "WorkflowAppWithScopedParameters";
  private static final String WORKFLOW_APP_WITH_SCOPED_PARAMETERS_WORKFLOW = "OneWorkflow";

  private static final String EMPTY_ARRAY_JSON = "[]";

  @Category(XSlowTests.class)
  @Test
  public void testProgramStartStopStatus() throws Exception {
    // deploy, check the status
    HttpResponse response = deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    // flow is stopped initially
    Assert.assertEquals(ProgramController.State.STOPPED.toString(),
                        getRunnableStatus(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(),
                                          WORDCOUNT_FLOW_NAME));

    // try a random action on the flow.
    Assert.assertEquals(405, getRunnableStartStop(TEST_NAMESPACE1, WORDCOUNT_APP_NAME,
                                                  ProgramType.FLOW.getCategoryName(), WORDCOUNT_FLOW_NAME, "random"));

    // flow is still stopped
    Assert.assertEquals(ProgramController.State.STOPPED.toString(),
                        getRunnableStatus(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(),
                                          WORDCOUNT_FLOW_NAME));

    // start flow in the wrong namespace and verify that it does not start
    Assert.assertEquals(404, getRunnableStartStop(TEST_NAMESPACE2, WORDCOUNT_APP_NAME,
                                                  ProgramType.FLOW.getCategoryName(), WORDCOUNT_FLOW_NAME, "start"));
    Assert.assertEquals(ProgramController.State.STOPPED.toString(),
                        getRunnableStatus(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(),
                                          WORDCOUNT_FLOW_NAME));

    // start a flow and check the status
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE1, WORDCOUNT_APP_NAME,
                                                  ProgramType.FLOW.getCategoryName(), WORDCOUNT_FLOW_NAME, "start"));
    waitState(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(), WORDCOUNT_FLOW_NAME,
              ProgramRunStatus.RUNNING.toString());

    // stop the flow and check the status
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE1, WORDCOUNT_APP_NAME,
                                                  ProgramType.FLOW.getCategoryName(), WORDCOUNT_FLOW_NAME, "stop"));
    waitState(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(), WORDCOUNT_FLOW_NAME,
              ProgramController.State.STOPPED.toString());

    // deploy another app in a different namespace and verify
    response = deploy(DummyAppWithTrackingTable.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    // mapreduce is stopped initially
    Assert.assertEquals(ProgramController.State.STOPPED.toString(),
                        getRunnableStatus(TEST_NAMESPACE2, DUMMY_APP_ID, ProgramType.MAPREDUCE.getCategoryName(),
                                          DUMMY_RUNNABLE_ID));

    // start mapreduce in the wrong namespace and verify it does not start
    Assert.assertEquals(404, getRunnableStartStop(TEST_NAMESPACE1, DUMMY_APP_ID,
                                                  ProgramType.MAPREDUCE.getCategoryName(), DUMMY_RUNNABLE_ID, "start"));
    Assert.assertEquals(ProgramController.State.STOPPED.toString(),
                        getRunnableStatus(TEST_NAMESPACE2, DUMMY_APP_ID, ProgramType.MAPREDUCE.getCategoryName(),
                                          DUMMY_RUNNABLE_ID));

    // start map-reduce and verify status
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE2, DUMMY_APP_ID,
                                                  ProgramType.MAPREDUCE.getCategoryName(), DUMMY_RUNNABLE_ID, "start"));
    waitState(TEST_NAMESPACE2, DUMMY_APP_ID, ProgramType.MAPREDUCE.getCategoryName(), DUMMY_RUNNABLE_ID,
              ProgramRunStatus.RUNNING.toString());

    // stop the mapreduce program and check the status
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE2, DUMMY_APP_ID,
                                                  ProgramType.MAPREDUCE.getCategoryName(), DUMMY_RUNNABLE_ID, "stop"));
    waitState(TEST_NAMESPACE2, DUMMY_APP_ID, ProgramType.MAPREDUCE.getCategoryName(), DUMMY_RUNNABLE_ID,
              ProgramController.State.STOPPED.toString());

    // deploy an app containing a workflow
    response = deploy(SleepingWorkflowApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    // workflow is stopped initially
    Assert.assertEquals(ProgramController.State.STOPPED.toString(),
                        getRunnableStatus(TEST_NAMESPACE2, SLEEP_WORKFLOW_APP_ID,
                                          ProgramType.WORKFLOW.getCategoryName(), SLEEP_WORKFLOW_RUNNABLE_ID));

    // start workflow in the wrong namespace and verify that it does not start
    Assert.assertEquals(404, getRunnableStartStop(TEST_NAMESPACE1, SLEEP_WORKFLOW_APP_ID,
                                                  ProgramType.WORKFLOW.getCategoryName(), SLEEP_WORKFLOW_RUNNABLE_ID,
                                                  "start"));
    Assert.assertEquals(ProgramController.State.STOPPED.toString(),
                        getRunnableStatus(TEST_NAMESPACE2, SLEEP_WORKFLOW_APP_ID,
                                          ProgramType.WORKFLOW.getCategoryName(), SLEEP_WORKFLOW_RUNNABLE_ID));

    // start workflow and check status
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE2, SLEEP_WORKFLOW_APP_ID,
                                                  ProgramType.WORKFLOW.getCategoryName(), SLEEP_WORKFLOW_RUNNABLE_ID,
                                                  "start"));
    waitState(TEST_NAMESPACE2, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW.getCategoryName(),
              SLEEP_WORKFLOW_RUNNABLE_ID, ProgramRunStatus.RUNNING.toString());

    // workflow will stop itself
    waitState(TEST_NAMESPACE2, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW.getCategoryName(),
              SLEEP_WORKFLOW_RUNNABLE_ID, ProgramController.State.STOPPED.toString());

    // cleanup
    response = doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE2));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  /**
   * Tests history of a flow.
   */
  @Test
  public void testFlowHistory() throws Exception {
    testHistory(WordCountApp.class, TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(),
                WORDCOUNT_FLOW_NAME);
  }

  /**
   * Tests history of a mapreduce.
   */
  @Category(XSlowTests.class)
  @Test
  public void testMapreduceHistory() throws Exception {
    testHistory(DummyAppWithTrackingTable.class, TEST_NAMESPACE2, DUMMY_APP_ID, ProgramType.MAPREDUCE.getCategoryName(),
                DUMMY_RUNNABLE_ID);
  }

  /**
   * Tests history of a workflow.
   */
  @Category(SlowTests.class)
  @Test
  public void testWorkflowHistory() throws Exception {
    try {
      deploy(SleepingWorkflowApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
      // first run
      Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE1, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW
        .getCategoryName(), SLEEP_WORKFLOW_RUNNABLE_ID, "start"));
      waitState(TEST_NAMESPACE1, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW.getCategoryName(),
                SLEEP_WORKFLOW_RUNNABLE_ID, ProgramRunStatus.RUNNING.toString());
      // workflow stops by itself after actions are done
      waitState(TEST_NAMESPACE1, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW.getCategoryName(),
                SLEEP_WORKFLOW_RUNNABLE_ID, ProgramController.State.STOPPED.toString());

      // second run
      Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE1, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW
        .getCategoryName(), SLEEP_WORKFLOW_RUNNABLE_ID, "start"));
      waitState(TEST_NAMESPACE1, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW.getCategoryName(),
                SLEEP_WORKFLOW_RUNNABLE_ID, ProgramRunStatus.RUNNING.toString());
      // workflow stops by itself after actions are done
      waitState(TEST_NAMESPACE1, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW.getCategoryName(),
                SLEEP_WORKFLOW_RUNNABLE_ID, ProgramController.State.STOPPED.toString());

      String url = String.format("apps/%s/%s/%s/runs?status=completed", SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW
        .getCategoryName(), SLEEP_WORKFLOW_RUNNABLE_ID);
      historyStatusWithRetry(getVersionedAPIPath(url, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1), 2);

    } finally {
      Assert.assertEquals(200, doDelete(getVersionedAPIPath("apps/" + SLEEP_WORKFLOW_APP_ID, Constants.Gateway
        .API_VERSION_3_TOKEN, TEST_NAMESPACE1)).getStatusLine().getStatusCode());
    }
  }

  @Test
  public void testFlowRuntimeArgs() throws Exception {
    testRuntimeArgs(WordCountApp.class, TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(),
                    WORDCOUNT_FLOW_NAME);
  }

  @Test
  public void testWorkflowRuntimeArgs() throws Exception {
    testRuntimeArgs(SleepingWorkflowApp.class, TEST_NAMESPACE2, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW
      .getCategoryName(), SLEEP_WORKFLOW_RUNNABLE_ID);
  }

  @Test
  public void testMapreduceRuntimeArgs() throws Exception {
    testRuntimeArgs(DummyAppWithTrackingTable.class, TEST_NAMESPACE1, DUMMY_APP_ID, ProgramType.MAPREDUCE
      .getCategoryName(), DUMMY_RUNNABLE_ID);
  }

  @Test
  public void testBatchStatus() throws Exception {
    final String statusUrl1 = getVersionedAPIPath("status", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    final String statusUrl2 = getVersionedAPIPath("status", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);

    // invalid json must return 400
    Assert.assertEquals(400, doPost(statusUrl1, "").getStatusLine().getStatusCode());
    Assert.assertEquals(400, doPost(statusUrl2, "").getStatusLine().getStatusCode());
    // empty array is valid args
    Assert.assertEquals(200, doPost(statusUrl1, EMPTY_ARRAY_JSON).getStatusLine().getStatusCode());
    Assert.assertEquals(200, doPost(statusUrl2, EMPTY_ARRAY_JSON).getStatusLine().getStatusCode());

    // deploy an app in namespace1
    deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    // deploy another app in namespace2
    deploy(AppWithServices.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);

    // data requires appId, programId, and programType. Test missing fields/invalid programType
    Assert.assertEquals(400, doPost(statusUrl1, "[{'appId':'WordCountApp', 'programType':'Flow'}]")
      .getStatusLine().getStatusCode());
    Assert.assertEquals(400, doPost(statusUrl1, "[{'appId':'WordCountApp', 'programId':'WordCountFlow'}]")
      .getStatusLine().getStatusCode());
    Assert.assertEquals(400, doPost(statusUrl1, "[{'programType':'Flow', 'programId':'WordCountFlow'}, {'appId':" +
      "'AppWithServices', 'programType': 'service', 'programId': 'NoOpService'}]").getStatusLine().getStatusCode());
    Assert.assertEquals(400,
                        doPost(statusUrl1, "[{'appId':'WordCountApp', 'programType':'Flow' " +
                          "'programId':'WordCountFlow'}]").getStatusLine().getStatusCode());
    // Test missing app, programType, etc
    List<JsonObject> returnedBody = readResponse(doPost(statusUrl1, "[{'appId':'NotExist', 'programType':'Flow', " +
      "'programId':'WordCountFlow'}]"), LIST_OF_JSONOBJECT_TYPE);
    Assert.assertEquals("App: NotExist not found", returnedBody.get(0).get("error").getAsString());
    returnedBody = readResponse(
      doPost(statusUrl1, "[{'appId':'WordCountApp', 'programType':'flow', 'programId':'NotExist'}," +
        "{'appId':'WordCountApp', 'programType':'flow', 'programId':'WordCountFlow'}]"), LIST_OF_JSONOBJECT_TYPE);
    Assert.assertEquals("Program not found", returnedBody.get(0).get("error").getAsString());
    // The programType should be consistent. Second object should have proper status
    Assert.assertEquals("Flow", returnedBody.get(1).get("programType").getAsString());
    Assert.assertEquals(ProgramController.State.STOPPED.toString(), returnedBody.get(1).get("status").getAsString());


    // test valid cases for namespace1
    HttpResponse response = doPost(statusUrl1,
                                   "[{'appId':'WordCountApp', 'programType':'Flow', 'programId':'WordCountFlow'}," +
                                     "{'appId': 'WordCountApp', 'programType': 'Procedure', 'programId': " +
                                     "'WordFrequency'}]");
    verifyInitialBatchStatusOutput(response);

    // test valid cases for namespace2
    response = doPost(statusUrl2, "[{'appId': 'AppWithServices', 'programType': 'Service', 'programId': " +
      "'NoOpService'}]");
    verifyInitialBatchStatusOutput(response);


    // start the flow
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW
      .getCategoryName(), WORDCOUNT_FLOW_NAME, "start"));
    // test status API after starting the flow
    response = doPost(statusUrl1, "[{'appId':'WordCountApp', 'programType':'Flow', 'programId':'WordCountFlow'}," +
      "{'appId': 'WordCountApp', 'programType': 'Mapreduce', 'programId': 'VoidMapReduceJob'}]");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    returnedBody = readResponse(response, LIST_OF_JSONOBJECT_TYPE);
    Assert.assertEquals(ProgramRunStatus.RUNNING.toString(), returnedBody.get(0).get("status").getAsString());
    Assert.assertEquals(ProgramController.State.STOPPED.toString(), returnedBody.get(1).get("status").getAsString());

    // start the service
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE2, APP_WITH_SERVICES_APP_ID, ProgramType.SERVICE
      .getCategoryName(), APP_WITH_SERVICES_SERVICE_NAME, "start"));
    // test status API after starting the service
    response = doPost(statusUrl2, "[{'appId': 'AppWithServices', 'programType': 'Service', 'programId': " +
      "'NoOpService'}]");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    returnedBody = readResponse(response, LIST_OF_JSONOBJECT_TYPE);
    Assert.assertEquals(ProgramRunStatus.RUNNING.toString(), returnedBody.get(0).get("status").getAsString());

    // stop the flow
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW
      .getCategoryName(), WORDCOUNT_FLOW_NAME, "stop"));
    waitState(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(), WORDCOUNT_FLOW_NAME,
              ProgramController.State.STOPPED.toString());
    // stop the service
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE2, APP_WITH_SERVICES_APP_ID, ProgramType.SERVICE
      .getCategoryName(), APP_WITH_SERVICES_SERVICE_NAME, "stop"));
    waitState(TEST_NAMESPACE2, APP_WITH_SERVICES_APP_ID, ProgramType.SERVICE.getCategoryName(),
              APP_WITH_SERVICES_SERVICE_NAME, ProgramController.State.STOPPED.toString());


    // try posting a status request with namespace2 for apps in namespace1
    response = doPost(statusUrl2, "[{'appId':'WordCountApp', 'programType':'Flow', 'programId':'WordCountFlow'}," +
      "{'appId': 'WordCountApp', 'programType': 'Procedure', 'programId': 'WordFrequency'}," +
      "{'appId': 'WordCountApp', 'programType': 'Mapreduce', 'programId': 'VoidMapReduceJob'}]");
    returnedBody = readResponse(response, LIST_OF_JSONOBJECT_TYPE);
    Assert.assertEquals("App: WordCountApp not found", returnedBody.get(0).get("error").getAsString());
    Assert.assertEquals("App: WordCountApp not found", returnedBody.get(1).get("error").getAsString());
    Assert.assertEquals("App: WordCountApp not found", returnedBody.get(2).get("error").getAsString());
  }

  @Test
  public void testBatchInstances() throws Exception {
    final String instancesUrl1 = getVersionedAPIPath("instances", Constants.Gateway.API_VERSION_3_TOKEN,
                                                     TEST_NAMESPACE1);
    final String instancesUrl2 = getVersionedAPIPath("instances", Constants.Gateway.API_VERSION_3_TOKEN,
                                                     TEST_NAMESPACE2);

    Assert.assertEquals(400, doPost(instancesUrl1, "").getStatusLine().getStatusCode());
    Assert.assertEquals(400, doPost(instancesUrl2, "").getStatusLine().getStatusCode());

    // empty array is valid args
    Assert.assertEquals(200, doPost(instancesUrl1, "[]").getStatusLine().getStatusCode());
    Assert.assertEquals(200, doPost(instancesUrl2, "[]").getStatusLine().getStatusCode());

    deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    deploy(AppWithServices.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);

    // data requires appId, programId, and programType. Test missing fields/invalid programType
    // TODO: These json strings should be replaced with JsonObjects so it becomes easier to refactor in future
    Assert.assertEquals(400, doPost(instancesUrl1, "[{'appId':'WordCountApp', 'programType':'Flow'}]")
      .getStatusLine().getStatusCode());
    Assert.assertEquals(400, doPost(instancesUrl1, "[{'appId':'WordCountApp', 'programId':'WordCountFlow'}]")
      .getStatusLine().getStatusCode());
    Assert.assertEquals(400, doPost(instancesUrl1, "[{'programType':'Flow', 'programId':'WordCountFlow'}," +
      "{'appId': 'WordCountApp', 'programType': 'procedure', 'programId': 'WordFrequency'}]")
      .getStatusLine().getStatusCode());
    Assert.assertEquals(400, doPost(instancesUrl1, "[{'appId':'WordCountApp', 'programType':'NotExist', " +
      "'programId':'WordCountFlow'}]").getStatusLine().getStatusCode());

    // Test malformed json
    Assert.assertEquals(400,
                        doPost(instancesUrl1,
                               "[{'appId':'WordCountApp', 'programType':'Flow' 'programId':'WordCountFlow'}]")
                          .getStatusLine().getStatusCode());

    // Test missing app, programType, etc
    List<JsonObject> returnedBody = readResponse(
      doPost(instancesUrl1, "[{'appId':'NotExist', 'programType':'Flow', 'programId':'WordCountFlow'}]"),
      LIST_OF_JSONOBJECT_TYPE);
    Assert.assertEquals(404, returnedBody.get(0).get("statusCode").getAsInt());
    returnedBody = readResponse(
      doPost(instancesUrl1, "[{'appId':'WordCountApp', 'programType':'flow', 'programId':'WordCountFlow', " +
        "'runnableId': " +
        "NotExist'}]"), LIST_OF_JSONOBJECT_TYPE);
    Assert.assertEquals(404, returnedBody.get(0).get("statusCode").getAsInt());


    // valid test in namespace1
    HttpResponse response = doPost(instancesUrl1,
                                   "[{'appId':'WordCountApp', 'programType':'Flow', 'programId':'WordCountFlow', " +
                                     "'runnableId': 'StreamSource'}," +
                                     "{'appId': 'WordCountApp', 'programType': 'Procedure', 'programId': " +
                                     "'WordFrequency'}]");

    verifyInitialBatchInstanceOutput(response);

    // valid test in namespace2
    response = doPost(instancesUrl2,
                      "[{'appId': 'AppWithServices', 'programType':'Service', 'programId':'NoOpService', " +
                        "'runnableId':'NoOpService'}]");
    verifyInitialBatchInstanceOutput(response);


    // start the flow
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW
      .getCategoryName(), WORDCOUNT_FLOW_NAME, "start"));
    response = doPost(instancesUrl1, "[{'appId':'WordCountApp', 'programType':'Flow', 'programId':'WordCountFlow'," +
      "'runnableId': 'StreamSource'}]");
    returnedBody = readResponse(response, LIST_OF_JSONOBJECT_TYPE);
    Assert.assertEquals(1, returnedBody.get(0).get("provisioned").getAsInt());

    // start the service
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE2, APP_WITH_SERVICES_APP_ID, ProgramType.SERVICE
      .getCategoryName(), APP_WITH_SERVICES_SERVICE_NAME, "start"));
    response = doPost(instancesUrl2, "[{'appId':'AppWithServices', 'programType':'Service','programId':'NoOpService'," +
      " 'runnableId':'NoOpService'}]");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    returnedBody = readResponse(response, LIST_OF_JSONOBJECT_TYPE);
    Assert.assertEquals(1, returnedBody.get(0).get("provisioned").getAsInt());

    // request for 2 more instances of the flowlet
    Assert.assertEquals(200, requestFlowletInstances(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, WORDCOUNT_FLOW_NAME,
                                                     WORDCOUNT_FLOWLET_NAME, 2));
    returnedBody = readResponse(doPost(instancesUrl1, "[{'appId':'WordCountApp', 'programType':'Flow'," +
      "'programId':'WordCountFlow', 'runnableId': 'StreamSource'}]"), LIST_OF_JSONOBJECT_TYPE);
    // verify that 2 more instances were requested
    Assert.assertEquals(2, returnedBody.get(0).get("requested").getAsInt());


    getRunnableStartStop(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(),
                         WORDCOUNT_FLOW_NAME, "stop");
    getRunnableStartStop(TEST_NAMESPACE2, APP_WITH_SERVICES_APP_ID, ProgramType.SERVICE.getCategoryName(),
                         APP_WITH_SERVICES_SERVICE_NAME, "stop");

    waitState(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(), WORDCOUNT_FLOW_NAME,
              ProgramController.State.STOPPED.toString());
    waitState(TEST_NAMESPACE2, APP_WITH_SERVICES_APP_ID, ProgramType.SERVICE.getCategoryName(),
              APP_WITH_SERVICES_SERVICE_NAME, ProgramController.State.STOPPED.toString());
  }

  /**
   * Tests for program list calls
   */
  @Test
  public void testProgramList() throws Exception {
    // test initial state
    testListInitialState(TEST_NAMESPACE1, ProgramType.FLOW);
    testListInitialState(TEST_NAMESPACE2, ProgramType.MAPREDUCE);
    testListInitialState(TEST_NAMESPACE1, ProgramType.WORKFLOW);
    testListInitialState(TEST_NAMESPACE2, ProgramType.SPARK);
    testListInitialState(TEST_NAMESPACE1, ProgramType.SERVICE);

    // deploy WordCountApp in namespace1 and verify
    HttpResponse response = deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    // deploy AppWithServices in namespace2 and verify
    response = deploy(AppWithServices.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    // verify list by namespace
    verifyProgramList(TEST_NAMESPACE1, null, ProgramType.FLOW.getCategoryName(), 1);
    verifyProgramList(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.MAPREDUCE.getCategoryName(), 1);
    verifyProgramList(TEST_NAMESPACE2, null, ProgramType.SERVICE.getCategoryName(), 1);

    // verify list by app
    verifyProgramList(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(), 1);
    verifyProgramList(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.MAPREDUCE.getCategoryName(), 1);
    verifyProgramList(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.WORKFLOW.getCategoryName(), 0);
    verifyProgramList(TEST_NAMESPACE2, APP_WITH_SERVICES_APP_ID, ProgramType.SERVICE.getCategoryName(), 1);

    // verify invalid namespace
    Assert.assertEquals(404, getProgramListResponseCode(TEST_NAMESPACE1, APP_WITH_SERVICES_APP_ID,
                                                        ProgramType.SERVICE.getCategoryName()));
    // verify invalid app
    Assert.assertEquals(404, getProgramListResponseCode(TEST_NAMESPACE1, "random", ProgramType.FLOW.getCategoryName()));

    // verify invalid program type
    Assert.assertEquals(404, getProgramListResponseCode(TEST_NAMESPACE2, APP_WITH_SERVICES_APP_ID, "random"));
  }

  /**
   * Worker Specification tests
   */
  @Test
  public void testWorkerSpecification() throws Exception {
    // deploy AppWithWorker in namespace1 and verify
    HttpResponse response = deploy(AppWithWorker.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    verifyProgramSpecification(TEST_NAMESPACE1, AppWithWorker.NAME, ProgramType.WORKER.getCategoryName(),
                               AppWithWorker.WORKER);
    Assert.assertEquals(404, getProgramSpecificationResponseCode(TEST_NAMESPACE2, AppWithWorker.NAME,
                                                                 ProgramType.WORKER.getCategoryName(),
                                                                 AppWithWorker.WORKER));
  }

  /**
   * Program specification tests through appfabric apis.
   */
  @Test
  public void testProgramSpecification() throws Exception {
    // deploy WordCountApp in namespace1 and verify
    HttpResponse response = deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    // deploy AppWithServices in namespace2 and verify
    response = deploy(AppWithServices.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    // deploy AppWithWorkflow in namespace2 and verify
    response = deploy(AppWithWorkflow.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    // deploy AppWithWorker in namespace1 and verify
    response = deploy(AppWithWorker.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    // verify program specification
    verifyProgramSpecification(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(),
                               WORDCOUNT_FLOW_NAME);
    verifyProgramSpecification(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.MAPREDUCE.getCategoryName(),
                               WORDCOUNT_MAPREDUCE_NAME);
    verifyProgramSpecification(TEST_NAMESPACE2, APP_WITH_SERVICES_APP_ID, ProgramType.SERVICE.getCategoryName(),
                               APP_WITH_SERVICES_SERVICE_NAME);
    verifyProgramSpecification(TEST_NAMESPACE2, APP_WITH_WORKFLOW_APP_ID, ProgramType.WORKFLOW.getCategoryName(),
                               APP_WITH_WORKFLOW_WORKFLOW_NAME);
    verifyProgramSpecification(TEST_NAMESPACE1, AppWithWorker.NAME, ProgramType.WORKER.getCategoryName(),
                               AppWithWorker.WORKER);

    // verify invalid namespace
    Assert.assertEquals(404, getProgramSpecificationResponseCode(TEST_NAMESPACE1, APP_WITH_SERVICES_APP_ID,
                                                                 ProgramType.SERVICE.getCategoryName(),
                                                                 APP_WITH_SERVICES_SERVICE_NAME));
    // verify invalid app
    Assert.assertEquals(404, getProgramSpecificationResponseCode(TEST_NAMESPACE2, APP_WITH_SERVICES_APP_ID,
                                                                 ProgramType.WORKFLOW.getCategoryName(),
                                                                 APP_WITH_WORKFLOW_WORKFLOW_NAME));
    // verify invalid runnable type
    Assert.assertEquals(405, getProgramSpecificationResponseCode(TEST_NAMESPACE2, APP_WITH_SERVICES_APP_ID,
                                                                 "random", APP_WITH_WORKFLOW_WORKFLOW_NAME));

    // verify invalid runnable type
    Assert.assertEquals(404, getProgramSpecificationResponseCode(TEST_NAMESPACE2, AppWithWorker.NAME,
                                                                 ProgramType.WORKER.getCategoryName(),
                                                                 AppWithWorker.WORKER));
  }

  @Test
  public void testFlows() throws Exception {
    // deploy WordCountApp in namespace1 and verify
    HttpResponse response = deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    // check initial flowlet instances
    int initial = getFlowletInstances(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, WORDCOUNT_FLOW_NAME, WORDCOUNT_FLOWLET_NAME);
    Assert.assertEquals(1, initial);

    // request two more instances
    Assert.assertEquals(200, requestFlowletInstances(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, WORDCOUNT_FLOW_NAME,
                                                     WORDCOUNT_FLOWLET_NAME, 3));
    // verify
    int after = getFlowletInstances(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, WORDCOUNT_FLOW_NAME, WORDCOUNT_FLOWLET_NAME);
    Assert.assertEquals(3, after);

    // invalid namespace
    Assert.assertEquals(404, requestFlowletInstances(TEST_NAMESPACE2, WORDCOUNT_APP_NAME, WORDCOUNT_FLOW_NAME,
                                                     WORDCOUNT_FLOWLET_NAME, 3));
    // invalid app
    Assert.assertEquals(404, requestFlowletInstances(TEST_NAMESPACE1, APP_WITH_SERVICES_APP_ID, WORDCOUNT_FLOW_NAME,
                                                     WORDCOUNT_FLOWLET_NAME, 3));
    // invalid flow
    Assert.assertEquals(404, requestFlowletInstances(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, "random",
                                                     WORDCOUNT_FLOWLET_NAME, 3));
    // invalid flowlet
    Assert.assertEquals(404, requestFlowletInstances(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, WORDCOUNT_FLOW_NAME,
                                                     "random", 3));

    // test live info
    // send invalid program type to live info
    response = sendLiveInfoRequest(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, "random", WORDCOUNT_FLOW_NAME);
    Assert.assertEquals(405, response.getStatusLine().getStatusCode());

    // test valid live info
    JsonObject liveInfo = getLiveInfo(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(),
                                      WORDCOUNT_FLOW_NAME);
    Assert.assertEquals(WORDCOUNT_APP_NAME, liveInfo.get("app").getAsString());
    Assert.assertEquals(ProgramType.FLOW.getPrettyName(), liveInfo.get("type").getAsString());
    Assert.assertEquals(WORDCOUNT_FLOW_NAME, liveInfo.get("id").getAsString());

    // start flow
    int status = getRunnableStartStop(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(),
                                      WORDCOUNT_FLOW_NAME, "start");
    Assert.assertEquals(200, status);

    liveInfo = getLiveInfo(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(),
                           WORDCOUNT_FLOW_NAME);
    Assert.assertEquals(WORDCOUNT_APP_NAME, liveInfo.get("app").getAsString());
    Assert.assertEquals(ProgramType.FLOW.getPrettyName(), liveInfo.get("type").getAsString());
    Assert.assertEquals(WORDCOUNT_FLOW_NAME, liveInfo.get("id").getAsString());
    Assert.assertEquals("in-memory", liveInfo.get("runtime").getAsString());

    // should not delete queues while running
    Assert.assertEquals(403, deleteQueues(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, WORDCOUNT_FLOW_NAME));
    Assert.assertEquals(403, deleteQueues(TEST_NAMESPACE1));

    // stop
    status = getRunnableStartStop(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(),
                                  WORDCOUNT_FLOW_NAME, "stop");
    Assert.assertEquals(200, status);
    // delete queues
    Assert.assertEquals(200, deleteQueues(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, WORDCOUNT_FLOW_NAME));
  }

  @Test
  public void testChangeFlowletStreamInput() throws Exception {
    final String appId = "MultiStreamApp";
    final String flowId = "CounterFlow";
    final String flowletId = "counter1";

    HttpResponse response = deploy(MultiStreamApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1, appId);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    // change stream input in the wrong namespace
    Assert.assertEquals(404, changeFLowletStreamConnection(TEST_NAMESPACE2, appId, flowId, flowletId, "stream1",
                                                           "stream2"));
    // change stream input to a non-existing stream in the right namespace
    Assert.assertEquals(404, changeFLowletStreamConnection(TEST_NAMESPACE1, appId, flowId, flowletId, "stream1",
                                                           "notfound"));

    Assert.assertEquals(200, changeFLowletStreamConnection(TEST_NAMESPACE1, appId, flowId, flowletId, "stream1",
                                                           "stream2"));
    // stream1 is no longer a connection
    Assert.assertEquals(500, changeFLowletStreamConnection(TEST_NAMESPACE1, appId, flowId, flowletId, "stream1",
                                                           "stream3"));
    Assert.assertEquals(200, changeFLowletStreamConnection(TEST_NAMESPACE1, appId, flowId, flowletId, "stream2",
                                                           "stream3"));

    Assert.assertEquals(200, changeFLowletStreamConnection(TEST_NAMESPACE1, appId, flowId, flowletId, "stream3",
                                                           "stream4"));
    // stream1 is no longer a connection
    Assert.assertEquals(500, changeFLowletStreamConnection(TEST_NAMESPACE1, appId, flowId, flowletId, "stream3",
                                                           "stream1"));
    Assert.assertEquals(200, changeFLowletStreamConnection(TEST_NAMESPACE1, appId, flowId, flowletId, "stream4",
                                                           "stream1"));
  }

  private void setAndTestRuntimeArgs(String namespace, String appId, String runnableType, String runnableId,
                              Map<String, String> args) throws Exception {
    HttpResponse response;
    String argString = GSON.toJson(args, new TypeToken<Map<String, String>>() { }.getType());
    String versionedRuntimeArgsUrl = getVersionedAPIPath("apps/" + appId + "/" + runnableType + "/" + runnableId +
                                                           "/runtimeargs", Constants.Gateway.API_VERSION_3_TOKEN,
                                                         namespace);
    response = doPut(versionedRuntimeArgsUrl, argString);

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doGet(versionedRuntimeArgsUrl);

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String responseEntity = EntityUtils.toString(response.getEntity());
    Map<String, String> argsRead = GSON.fromJson(responseEntity, new TypeToken<Map<String, String>>() { }.getType());

    Assert.assertEquals(args.size(), argsRead.size());
  }

  @Category(XSlowTests.class)
  @Test
  public void testMultipleWorkflowInstances() throws Exception {
    HttpResponse response = deploy(ConcurrentWorkflowApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Map<String, String> propMap = Maps.newHashMap();
    propMap.put(ProgramOptionConstants.CONCURRENT_RUNS_ENABLED, "true");
    PreferencesStore store = getInjector().getInstance(PreferencesStore.class);
    store.setProperties(TEST_NAMESPACE2, APP_WITH_CONCURRENT_WORKFLOW, ProgramType.WORKFLOW.getCategoryName(),
                        CONCURRENT_WORKFLOW_NAME, propMap);

    String runsUrl = getRunsUrl(TEST_NAMESPACE2, APP_WITH_CONCURRENT_WORKFLOW, CONCURRENT_WORKFLOW_NAME, "running");

    List<Map<String, String>> historyRuns = scheduleHistoryRuns(60, runsUrl, 1);
    // Two instances of the ConcurrentWorkflow should be RUNNING
    Assert.assertTrue(historyRuns.size() >= 2);

    // Suspend ConcurrentWorkflow schedules
    List<ScheduleSpecification> schedules = getSchedules(TEST_NAMESPACE2, APP_WITH_CONCURRENT_WORKFLOW,
                                                         CONCURRENT_WORKFLOW_NAME);

    for (ScheduleSpecification spec : schedules) {
      Assert.assertEquals(200, suspendSchedule(TEST_NAMESPACE2, APP_WITH_CONCURRENT_WORKFLOW,
                                               spec.getSchedule().getName()));
    }

    // delete the application
    String deleteUrl = getVersionedAPIPath("apps/" + APP_WITH_CONCURRENT_WORKFLOW, Constants.Gateway
      .API_VERSION_3_TOKEN, TEST_NAMESPACE2);
    deleteApplication(60, deleteUrl, 200);
  }

  private String createInput(String folderName) throws IOException {
    File inputDir = tmpFolder.newFolder(folderName);

    File inputFile = new File(inputDir.getPath() + "/words.txt");
    inputFile.deleteOnExit();
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

  @Category(XSlowTests.class)
  @Test
  public void testWorkflowScopedArguments() throws Exception {
    HttpResponse response = deploy(WorkflowAppWithScopedParameters.class, Constants.Gateway.API_VERSION_3_TOKEN,
                                   TEST_NAMESPACE2);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

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

    setAndTestRuntimeArgs(TEST_NAMESPACE2, WORKFLOW_APP_WITH_SCOPED_PARAMETERS, ProgramType.WORKFLOW.getCategoryName(),
                          WORKFLOW_APP_WITH_SCOPED_PARAMETERS_WORKFLOW, runtimeArguments);


    // Start the workflow
    int code = getRunnableStartStop(TEST_NAMESPACE2, WORKFLOW_APP_WITH_SCOPED_PARAMETERS,
                                    ProgramType.WORKFLOW.getCategoryName(),
                                    WORKFLOW_APP_WITH_SCOPED_PARAMETERS_WORKFLOW, "start");
    Assert.assertEquals(200, code);

    String runsUrl = getRunsUrl(TEST_NAMESPACE2, WORKFLOW_APP_WITH_SCOPED_PARAMETERS,
                                WORKFLOW_APP_WITH_SCOPED_PARAMETERS_WORKFLOW, "completed");
    scheduleHistoryRuns(180, runsUrl, 0);
  }

  @Category(XSlowTests.class)
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

    //TODO: cannot test the /current endpoint because of CDAP-66. Enable after that bug is fixed.
//    Assert.assertEquals(200, getWorkflowCurrentStatus(TEST_NAMESPACE2, APP_WITH_SCHEDULE_APP_NAME,
//                                                      APP_WITH_SCHEDULE_WORKFLOW_NAME));

    Map<String, String> runtimeArguments = Maps.newHashMap();
    runtimeArguments.put("someKey", "someWorkflowValue");
    runtimeArguments.put("workflowKey", "workflowValue");

    setAndTestRuntimeArgs(TEST_NAMESPACE2, APP_WITH_SCHEDULE_APP_NAME, ProgramType.WORKFLOW.getCategoryName(),
                          APP_WITH_SCHEDULE_WORKFLOW_NAME, runtimeArguments);

    // get schedules
    List<ScheduleSpecification> schedules = getSchedules(TEST_NAMESPACE2, APP_WITH_SCHEDULE_APP_NAME,
                                                         APP_WITH_SCHEDULE_WORKFLOW_NAME);
    Assert.assertEquals(1, schedules.size());
    String scheduleName = schedules.get(0).getSchedule().getName();
    Assert.assertNotNull(scheduleName);
    Assert.assertFalse(scheduleName.isEmpty());

    long current = System.currentTimeMillis();
    Long nextRunTime = getNextScheduledRunTime(TEST_NAMESPACE2, APP_WITH_SCHEDULE_APP_NAME,
                                               APP_WITH_SCHEDULE_WORKFLOW_NAME, scheduleName);
    Assert.assertNotNull(nextRunTime);
    Assert.assertTrue(nextRunTime > current);

    String runsUrl = getRunsUrl(TEST_NAMESPACE2, APP_WITH_SCHEDULE_APP_NAME, APP_WITH_SCHEDULE_WORKFLOW_NAME,
                                "completed");
    scheduleHistoryRuns(5, runsUrl, 0);

    //Check schedule status
    String statusUrl = getStatusUrl(TEST_NAMESPACE2, APP_WITH_SCHEDULE_APP_NAME, scheduleName);
    scheduleStatusCheck(5, statusUrl, "SCHEDULED");

    Assert.assertEquals(200, suspendSchedule(TEST_NAMESPACE2, APP_WITH_SCHEDULE_APP_NAME, scheduleName));
    //check paused state
    scheduleStatusCheck(5, statusUrl, "SUSPENDED");

    TimeUnit.SECONDS.sleep(2); //wait till any running jobs just before suspend call completes.

    int workflowRuns = getRuns(runsUrl);

    //Sleep for some time and verify there are no more scheduled jobs after the suspend.
    TimeUnit.SECONDS.sleep(10);

    int workflowRunsAfterSuspend = getRuns(runsUrl);
    Assert.assertEquals(workflowRuns, workflowRunsAfterSuspend);

    Assert.assertEquals(200, resumeSchedule(TEST_NAMESPACE2, APP_WITH_SCHEDULE_APP_NAME, scheduleName));

    scheduleHistoryRuns(5, runsUrl, workflowRunsAfterSuspend);

    //check scheduled state
    scheduleStatusCheck(5, statusUrl, "SCHEDULED");

    //Check status of a non existing schedule
    String invalid = getStatusUrl(TEST_NAMESPACE2, APP_WITH_SCHEDULE_APP_NAME, "invalid");
    scheduleStatusCheck(5, invalid, "NOT_FOUND");

    Assert.assertEquals(200, suspendSchedule(TEST_NAMESPACE2, APP_WITH_SCHEDULE_APP_NAME, scheduleName));

    //check paused state
    scheduleStatusCheck(5, statusUrl, "SUSPENDED");

    //Schedule operations using invalid namespace
    String inValidNamespaceUrl = getStatusUrl(TEST_NAMESPACE1, APP_WITH_SCHEDULE_APP_NAME, scheduleName);
    scheduleStatusCheck(5, inValidNamespaceUrl, "NOT_FOUND");
    Assert.assertEquals(404, suspendSchedule(TEST_NAMESPACE1, APP_WITH_SCHEDULE_APP_NAME, scheduleName));
    Assert.assertEquals(404, resumeSchedule(TEST_NAMESPACE1, APP_WITH_SCHEDULE_APP_NAME, scheduleName));

    TimeUnit.SECONDS.sleep(2); //wait till any running jobs just before suspend call completes.
  }

  @Category(XSlowTests.class)
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
                                   Constants.DEFAULT_NAMESPACE);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    // get schedules
    List<ScheduleSpecification> schedules = getSchedules(Constants.DEFAULT_NAMESPACE, APP_WITH_STREAM_SCHEDULE_APP_NAME,
                                                         APP_WITH_STREAM_SCHEDULE_WORKFLOW_NAME);
    Assert.assertEquals(2, schedules.size());
    String scheduleName1 = schedules.get(0).getSchedule().getName();
    String scheduleName2 = schedules.get(1).getSchedule().getName();
    Assert.assertNotNull(scheduleName1);
    Assert.assertFalse(scheduleName1.isEmpty());

    // Change notification threshold for stream
    response = doPut(String.format("/v3/namespaces/%s/streams/%s/config", Constants.DEFAULT_NAMESPACE,
                                   APP_WITH_STREAM_SCHEDULE_STREAM_NAME),
                     "{'notification.threshold.mb': 1}");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = doGet(String.format("/v3/namespaces/%s/streams/%s/info", Constants.DEFAULT_NAMESPACE,
                                   APP_WITH_STREAM_SCHEDULE_STREAM_NAME));
    String json = EntityUtils.toString(response.getEntity());
    StreamProperties properties = new Gson().fromJson(json, StreamProperties.class);
    Assert.assertEquals(1, properties.getNotificationThresholdMB().intValue());

    // Ingest over 1MB of data in stream
    for (int i = 0; i < 12; ++i) {
      response = doPost(String.format("/v3/namespaces/%s/streams/%s", Constants.DEFAULT_NAMESPACE,
                                      APP_WITH_STREAM_SCHEDULE_STREAM_NAME),
                        longString);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    }

    TimeUnit.SECONDS.sleep(10);
    String runsUrl = getRunsUrl(Constants.DEFAULT_NAMESPACE, APP_WITH_STREAM_SCHEDULE_APP_NAME,
                                APP_WITH_STREAM_SCHEDULE_WORKFLOW_NAME,
                                "completed");
    scheduleHistoryRuns(5, runsUrl, 0);

    //Check schedule status
    String statusUrl1 = getStatusUrl(Constants.DEFAULT_NAMESPACE, APP_WITH_STREAM_SCHEDULE_APP_NAME, scheduleName1);
    String statusUrl2 = getStatusUrl(Constants.DEFAULT_NAMESPACE, APP_WITH_STREAM_SCHEDULE_APP_NAME, scheduleName2);
    scheduleStatusCheck(5, statusUrl1, "SCHEDULED");
    scheduleStatusCheck(5, statusUrl2, "SCHEDULED");

    Assert.assertEquals(200, suspendSchedule(Constants.DEFAULT_NAMESPACE, APP_WITH_STREAM_SCHEDULE_APP_NAME,
                                             scheduleName1));
    Assert.assertEquals(200, suspendSchedule(Constants.DEFAULT_NAMESPACE, APP_WITH_STREAM_SCHEDULE_APP_NAME,
                                             scheduleName2));
    //check paused state
    scheduleStatusCheck(5, statusUrl1, "SUSPENDED");
    scheduleStatusCheck(5, statusUrl2, "SUSPENDED");

    TimeUnit.SECONDS.sleep(2); //wait till any running jobs just before suspend call completes.

    int workflowRuns = getRuns(runsUrl);

    // Sleep for some time and verify there are no more scheduled jobs after the suspend.
    for (int i = 0; i < 12; ++i) {
      response = doPost(String.format("/v3/namespaces/%s/streams/%s", Constants.DEFAULT_NAMESPACE,
                                      APP_WITH_STREAM_SCHEDULE_STREAM_NAME),
                        longString);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    }
    TimeUnit.SECONDS.sleep(5);

    int workflowRunsAfterSuspend = getRuns(runsUrl);
    Assert.assertEquals(workflowRuns, workflowRunsAfterSuspend);

    Assert.assertEquals(200, resumeSchedule(Constants.DEFAULT_NAMESPACE, APP_WITH_STREAM_SCHEDULE_APP_NAME,
                                            scheduleName1));

    scheduleHistoryRuns(5, runsUrl, workflowRunsAfterSuspend);

    //check scheduled state
    scheduleStatusCheck(5, statusUrl1, "SCHEDULED");

    //Check status of a non existing schedule
    String invalid = getStatusUrl(Constants.DEFAULT_NAMESPACE, APP_WITH_STREAM_SCHEDULE_APP_NAME, "invalid");
    scheduleStatusCheck(5, invalid, "NOT_FOUND");

    Assert.assertEquals(200, suspendSchedule("default", APP_WITH_STREAM_SCHEDULE_APP_NAME, scheduleName1));

    //check paused state
    scheduleStatusCheck(5, statusUrl1, "SUSPENDED");

    //Schedule operations using invalid namespace
    String inValidNamespaceUrl = getStatusUrl(TEST_NAMESPACE1, APP_WITH_STREAM_SCHEDULE_APP_NAME, scheduleName1);
    scheduleStatusCheck(5, inValidNamespaceUrl, "NOT_FOUND");
    Assert.assertEquals(404, suspendSchedule(TEST_NAMESPACE1, APP_WITH_STREAM_SCHEDULE_APP_NAME, scheduleName1));
    Assert.assertEquals(404, resumeSchedule(TEST_NAMESPACE1, APP_WITH_STREAM_SCHEDULE_APP_NAME, scheduleName1));

    TimeUnit.SECONDS.sleep(2); //wait till any running jobs just before suspend call completes.
  }

  @Category(XSlowTests.class)
  @Test
  public void testWorkflowRuns() throws Exception {
    HttpResponse response = deploy(WorkflowAppWithErrorRuns.class, Constants.Gateway.API_VERSION_3_TOKEN,
                                   TEST_NAMESPACE2);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    String runsUrl = getRunsUrl(TEST_NAMESPACE2, WORKFLOW_APP_WITH_ERROR_RUNS, WORKFLOW_WITH_ERROR_RUNS, "completed");
    scheduleHistoryRuns(5, runsUrl, 0);

    Map<String, String> propMap = ImmutableMap.of("ThrowError", "true");
    PreferencesStore store = getInjector().getInstance(PreferencesStore.class);
    store.setProperties(TEST_NAMESPACE2, WORKFLOW_APP_WITH_ERROR_RUNS, ProgramType.WORKFLOW.getCategoryName(),
                        WORKFLOW_WITH_ERROR_RUNS, propMap);

    runsUrl = getRunsUrl(TEST_NAMESPACE2, WORKFLOW_APP_WITH_ERROR_RUNS, WORKFLOW_WITH_ERROR_RUNS, "failed");
    scheduleHistoryRuns(5, runsUrl, 0);
  }

  @Test
  public void testMultipleWorkflowSchedules() throws Exception {
    // Deploy the app
    HttpResponse response = deploy(AppWithMultipleScheduledWorkflows.class, Constants.Gateway.API_VERSION_3_TOKEN,
                                   TEST_NAMESPACE2);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    List<ScheduleSpecification> someSchedules = getSchedules(TEST_NAMESPACE2, APP_WITH_MULTIPLE_WORKFLOWS_APP_NAME,
                                                         APP_WITH_MULTIPLE_WORKFLOWS_SOMEWORKFLOW);
    Assert.assertEquals(2, someSchedules.size());
    Assert.assertEquals(APP_WITH_MULTIPLE_WORKFLOWS_SOMEWORKFLOW, someSchedules.get(0).getProgram().getProgramName());
    Assert.assertEquals(APP_WITH_MULTIPLE_WORKFLOWS_SOMEWORKFLOW, someSchedules.get(1).getProgram().getProgramName());


    List<ScheduleSpecification> anotherSchedules = getSchedules(TEST_NAMESPACE2, APP_WITH_MULTIPLE_WORKFLOWS_APP_NAME,
                                                         APP_WITH_MULTIPLE_WORKFLOWS_ANOTHERWORKFLOW);
    Assert.assertEquals(3, anotherSchedules.size());
    Assert.assertEquals(APP_WITH_MULTIPLE_WORKFLOWS_ANOTHERWORKFLOW,
                        anotherSchedules.get(0).getProgram().getProgramName());
    Assert.assertEquals(APP_WITH_MULTIPLE_WORKFLOWS_ANOTHERWORKFLOW,
                        anotherSchedules.get(1).getProgram().getProgramName());
    Assert.assertEquals(APP_WITH_MULTIPLE_WORKFLOWS_ANOTHERWORKFLOW,
                        anotherSchedules.get(2).getProgram().getProgramName());
  }

  @Test
  public void testServices() throws Exception {
    HttpResponse response = deploy(AppWithServices.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    
    // start service in wrong namespace
    int code = getRunnableStartStop(TEST_NAMESPACE1, APP_WITH_SERVICES_APP_ID,
                                    ProgramType.SERVICE.getCategoryName(), APP_WITH_SERVICES_SERVICE_NAME, "start");
    Assert.assertEquals(404, code);

    code = getRunnableStartStop(TEST_NAMESPACE2, APP_WITH_SERVICES_APP_ID,
                                ProgramType.SERVICE.getCategoryName(), APP_WITH_SERVICES_SERVICE_NAME, "start");
    Assert.assertEquals(200, code);

    // verify instances
    try {
      getServiceInstances(TEST_NAMESPACE1, APP_WITH_SERVICES_APP_ID,
                          APP_WITH_SERVICES_SERVICE_NAME);
      Assert.fail("Should not find service in " + TEST_NAMESPACE1);
    } catch (AssertionError e) {
    }
    ServiceInstances instances = getServiceInstances(TEST_NAMESPACE2, APP_WITH_SERVICES_APP_ID,
                                                     APP_WITH_SERVICES_SERVICE_NAME);
    Assert.assertEquals(1, instances.getRequested());
    Assert.assertEquals(1, instances.getProvisioned());

    // request 2 additional instances
    code = setServiceInstances(TEST_NAMESPACE1, APP_WITH_SERVICES_APP_ID, APP_WITH_SERVICES_SERVICE_NAME, 3);
    Assert.assertEquals(404, code);
    code = setServiceInstances(TEST_NAMESPACE2, APP_WITH_SERVICES_APP_ID, APP_WITH_SERVICES_SERVICE_NAME, 3);
    Assert.assertEquals(200, code);

    // verify that additional instances were provisioned
    instances = getServiceInstances(TEST_NAMESPACE2, APP_WITH_SERVICES_APP_ID,
                                                     APP_WITH_SERVICES_SERVICE_NAME);
    Assert.assertEquals(3, instances.getRequested());
    Assert.assertEquals(3, instances.getProvisioned());

    // verify that endpoints are not available in the wrong namespace
    response = callService(TEST_NAMESPACE1, APP_WITH_SERVICES_APP_ID, APP_WITH_SERVICES_SERVICE_NAME, HttpMethod.POST,
                       "multi");
    code = response.getStatusLine().getStatusCode();
    Assert.assertEquals(404, code);

    response = callService(TEST_NAMESPACE1, APP_WITH_SERVICES_APP_ID, APP_WITH_SERVICES_SERVICE_NAME, HttpMethod.GET,
                       "multi/ping");
    code = response.getStatusLine().getStatusCode();
    Assert.assertEquals(404, code);

    // stop service
    code = getRunnableStartStop(TEST_NAMESPACE1, APP_WITH_SERVICES_APP_ID, ProgramType.SERVICE.getCategoryName(),
                                APP_WITH_SERVICES_SERVICE_NAME, "stop");
    Assert.assertEquals(404, code);

    code = getRunnableStartStop(TEST_NAMESPACE2, APP_WITH_SERVICES_APP_ID, ProgramType.SERVICE.getCategoryName(),
                                APP_WITH_SERVICES_SERVICE_NAME, "stop");
    Assert.assertEquals(200, code);
  }

  @Test
  public void testDeleteQueues() throws Exception {
    QueueName queueName = QueueName.fromFlowlet(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, WORDCOUNT_FLOW_NAME,
                                                WORDCOUNT_FLOWLET_NAME, "out");

    // enqueue some data
    enqueue(queueName, new QueueEntry("x".getBytes(Charsets.UTF_8)));

    // verify it exists
    Assert.assertTrue(dequeueOne(queueName));

    // clear queue in wrong namespace
    Assert.assertEquals(200, doDelete("/v3/namespaces/" + TEST_NAMESPACE2 + "/queues").getStatusLine().getStatusCode());
    // verify queue is still here
    Assert.assertTrue(dequeueOne(queueName));

    // clear queue in the right namespace
    Assert.assertEquals(200, doDelete("/v3/namespaces/" + TEST_NAMESPACE1 + "/queues").getStatusLine().getStatusCode());

    // verify queue is gone
    Assert.assertFalse(dequeueOne(queueName));
  }

  @After
  public void cleanup() throws Exception {
    doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2));
  }

  // TODO: Duplicate from AppFabricHttpHandlerTest, remove the AppFabricHttpHandlerTest method after deprecating v2 APIs
  private  void enqueue(QueueName queueName, final QueueEntry queueEntry) throws Exception {
    QueueClientFactory queueClientFactory = AppFabricTestBase.getInjector().getInstance(QueueClientFactory.class);
    final QueueProducer producer = queueClientFactory.createProducer(queueName);
    // doing inside tx
    TransactionExecutorFactory txExecutorFactory =
      AppFabricTestBase.getInjector().getInstance(TransactionExecutorFactory.class);
    txExecutorFactory.createExecutor(ImmutableList.of((TransactionAware) producer))
      .execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          // write more than one so that we can dequeue multiple times for multiple checks
          // we only dequeue twice, but ensure that the drop queues call drops the rest of the entries as well
          int numEntries = 0;
          while (numEntries++ < 5) {
            producer.enqueue(queueEntry);
          }
        }
      });
  }

  private boolean dequeueOne(QueueName queueName) throws Exception {
    QueueClientFactory queueClientFactory = AppFabricTestBase.getInjector().getInstance(QueueClientFactory.class);
    final QueueConsumer consumer = queueClientFactory.createConsumer(queueName,
                                                                     new ConsumerConfig(1L, 0, 1,
                                                                                        DequeueStrategy.ROUND_ROBIN,
                                                                                        null),
                                                                     1);
    // doing inside tx
    TransactionExecutorFactory txExecutorFactory =
      AppFabricTestBase.getInjector().getInstance(TransactionExecutorFactory.class);
    return txExecutorFactory.createExecutor(ImmutableList.of((TransactionAware) consumer))
      .execute(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return !consumer.dequeue(1).isEmpty();
        }
      });
  }

  private ServiceInstances getServiceInstances(String namespace, String app, String service) throws Exception {
    String instanceUrl = String.format("apps/%s/services/%s/instances", app, service);
    String versionedInstanceUrl = getVersionedAPIPath(instanceUrl, Constants.Gateway.API_VERSION_3_TOKEN, namespace);
    HttpResponse response = doGet(versionedInstanceUrl);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    ServiceInstances instances = readResponse(response, ServiceInstances.class);
    return instances;
  }

  private int setServiceInstances(String namespace, String app, String service, int instances) throws Exception {
    String instanceUrl = String.format("apps/%s/services/%s/instances", app, service);
    String versionedInstanceUrl = getVersionedAPIPath(instanceUrl, Constants.Gateway.API_VERSION_3_TOKEN, namespace);
    String instancesBody = GSON.toJson(new Instances(instances));
    return doPut(versionedInstanceUrl, instancesBody).getStatusLine().getStatusCode();
  }

  private HttpResponse callService(String namespace, String app, String service, HttpMethod method, String endpoint)
    throws Exception {
    String serviceUrl = String.format("apps/%s/service/%s/methods/%s", app, service, endpoint);
    String versionedServiceUrl = getVersionedAPIPath(serviceUrl, Constants.Gateway.API_VERSION_3_TOKEN, namespace);
    HttpResponse response;
    if (HttpMethod.GET.equals(method)) {
      response = doGet(versionedServiceUrl);
    } else if (HttpMethod.POST.equals(method)) {
      response = doPost(versionedServiceUrl);
    } else {
      throw new IllegalArgumentException("Only GET and POST supported right now.");
    }
    return response;
  }

  private int getWorkflowCurrentStatus(String namespace, String app, String workflow) throws Exception {
    String currentUrl = String.format("apps/%s/workflows/%s/current", app, workflow);
    String versionedUrl = getVersionedAPIPath(currentUrl, Constants.Gateway.API_VERSION_3_TOKEN, namespace);
    return doGet(versionedUrl).getStatusLine().getStatusCode();
  }

  private Long getNextScheduledRunTime(String namespace, String app, String workflow, String schedule)
    throws Exception {
    String nextRunTimeUrl = String.format("apps/%s/workflows/%s/nextruntime", app, workflow);
    String versionedUrl = getVersionedAPIPath(nextRunTimeUrl, Constants.Gateway.API_VERSION_3_TOKEN, namespace);
    HttpResponse response = doGet(versionedUrl);
    JsonArray array = readResponse(response, JsonArray.class);
    JsonObject wfObject = (JsonObject) array.get(0);
    Assert.assertNotNull(wfObject);
    String id = wfObject.get("id").getAsString();
    Long time = wfObject.get("time").getAsLong();
    Assert.assertTrue(id.contains(schedule));
    return time;
  }

  private String getStatusUrl(String namespace, String appName, String schedule) throws Exception {
    String statusUrl = String.format("apps/%s/schedules/%s/status", appName, schedule);
    return getVersionedAPIPath(statusUrl, Constants.Gateway.API_VERSION_3_TOKEN, namespace);
  }

  private int resumeSchedule(String namespace, String appName, String schedule) throws Exception {
    String scheduleResume = String.format("apps/%s/schedules/%s/resume", appName, schedule);
    HttpResponse response = doPost(getVersionedAPIPath(scheduleResume, Constants.Gateway.API_VERSION_3_TOKEN,
                                                       namespace));
    return response.getStatusLine().getStatusCode();
  }

  private int suspendSchedule(String namespace, String appName, String schedule) throws Exception {
    String scheduleSuspend = String.format("apps/%s/schedules/%s/suspend", appName, schedule);
    String versionedScheduledSuspend = getVersionedAPIPath(scheduleSuspend, Constants.Gateway.API_VERSION_3_TOKEN,
                                                           namespace);
    HttpResponse response = doPost(versionedScheduledSuspend);
    return response.getStatusLine().getStatusCode();
  }

  private int getRuns(String runsUrl) throws Exception {
    HttpResponse response = doGet(runsUrl);
    String json = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> history = GSON.fromJson(json, LIST_MAP_STRING_STRING_TYPE);
    return history.size();
  }

  private String getRunsUrl(String namespace, String appName, String workflow, String status) {
    String runsUrl = String.format("apps/%s/workflows/%s/runs?status=%s", appName, workflow, status);
    return getVersionedAPIPath(runsUrl, Constants.Gateway.API_VERSION_3_TOKEN, namespace);
  }

  private List<ScheduleSpecification> getSchedules(String namespace, String appName, String workflowName)
    throws Exception {
    String schedulesUrl = String.format("apps/%s/workflows/%s/schedules", appName, workflowName);
    String versionedUrl = getVersionedAPIPath(schedulesUrl, Constants.Gateway.API_VERSION_3_TOKEN, namespace);
    HttpResponse response = doGet(versionedUrl);
    String json = EntityUtils.toString(response.getEntity());
    return GSON.fromJson(json, new TypeToken<List<ScheduleSpecification>>() {
    }.getType());
  }

  private int deleteQueues(String namespace) throws Exception {
    String versionedDeleteUrl = getVersionedAPIPath("queues", Constants.Gateway.API_VERSION_3_TOKEN, namespace);
    HttpResponse response = doDelete(versionedDeleteUrl);
    return response.getStatusLine().getStatusCode();
  }

  private int deleteQueues(String namespace, String appId, String flow) throws Exception {
    String deleteQueuesUrl = String.format("apps/%s/flows/%s/queues", appId, flow);
    String versionedDeleteUrl = getVersionedAPIPath(deleteQueuesUrl, Constants.Gateway.API_VERSION_3_TOKEN, namespace);
    HttpResponse response = doDelete(versionedDeleteUrl);
    return response.getStatusLine().getStatusCode();
  }

  private JsonObject getLiveInfo(String namespace, String appId, String programType, String programId)
    throws Exception {
    HttpResponse response = sendLiveInfoRequest(namespace, appId, programType, programId);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return readResponse(response, JsonObject.class);
  }

  private HttpResponse sendLiveInfoRequest(String namespace, String appId, String programType, String programId)
    throws Exception {
    String liveInfoUrl = String.format("apps/%s/%s/%s/live-info", appId, programType, programId);
    String versionedUrl = getVersionedAPIPath(liveInfoUrl, Constants.Gateway.API_VERSION_3_TOKEN, namespace);
    return doGet(versionedUrl);
  }

  private int changeFLowletStreamConnection(String namespace, String appId, String flow, String flowlet,
                                            String oldStreamId, String newStreamId)
    throws Exception {
    String flowletStreamConnectionUrl = String.format("apps/%s/flows/%s/flowlets/%s/connections/%s", appId, flow,
                                                      flowlet, newStreamId);
    String versionedUrl = getVersionedAPIPath(flowletStreamConnectionUrl, Constants.Gateway.API_VERSION_3_TOKEN,
                                              namespace);
    JsonObject oldStream = new JsonObject();
    oldStream.addProperty("oldStreamId", oldStreamId);
    HttpResponse response = doPut(versionedUrl, GSON.toJson(oldStream));
    return response.getStatusLine().getStatusCode();
  }

  private int requestFlowletInstances(String namespace, String appId, String flow, String flowlet, int noRequested)
    throws Exception {
    String flowletInstancesVersionedUrl = getFlowletInstancesVersionedUrl(namespace, appId, flow, flowlet);
    JsonObject instances = new JsonObject();
    instances.addProperty("instances", noRequested);
    String body = GSON.toJson(instances);
    return doPut(flowletInstancesVersionedUrl, body).getStatusLine().getStatusCode();
  }

  private int getFlowletInstances(String namespace, String appId, String flow, String flowlet) throws Exception {
    String flowletInstancesUrl = getFlowletInstancesVersionedUrl(namespace, appId, flow, flowlet);
    String response = readResponse(doGet(flowletInstancesUrl));
    Assert.assertNotNull(response);
    JsonObject instances = GSON.fromJson(response, JsonObject.class);
    Assert.assertTrue(instances.has("instances"));
    return instances.get("instances").getAsInt();
  }

  private String getFlowletInstancesVersionedUrl(String namespace, String appId, String flow, String flowlet) {
    String flowletInstancesUrl = String.format("apps/%s/%s/%s/flowlets/%s/instances", appId,
                                               ProgramType.FLOW.getCategoryName(), flow, flowlet);
    return getVersionedAPIPath(flowletInstancesUrl, Constants.Gateway.API_VERSION_3_TOKEN, namespace);
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
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String result = EntityUtils.toString(response.getEntity());
    Assert.assertNotNull(result);
    return GSON.fromJson(result, JsonObject.class);
  }

  private int getProgramSpecificationResponseCode(String namespace, String appId, String programType, String programId)
    throws Exception {
    HttpResponse response = requestProgramSpecification(namespace, appId, programType, programId);
    return response.getStatusLine().getStatusCode();
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
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertEquals(EMPTY_ARRAY_JSON, readResponse(response));
  }

  private void verifyProgramList(String namespace, @Nullable String appName, String programType,
                                 int expected) throws Exception {
    HttpResponse response = requestProgramList(namespace, appName, programType);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String json = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> programs = GSON.fromJson(json, LIST_MAP_STRING_STRING_TYPE);
    Assert.assertEquals(expected, programs.size());
  }

  private int getProgramListResponseCode(String namespace, @Nullable String appName, String programType)
    throws Exception {
    HttpResponse response = requestProgramList(namespace, appName, programType);
    return response.getStatusLine().getStatusCode();
  }

  private HttpResponse requestProgramList(String namespace, @Nullable String appName, String programType)
    throws Exception {
    String uri;
    if (appName == null) {
      uri = getVersionedAPIPath(programType, Constants.Gateway.API_VERSION_3_TOKEN, namespace);
    } else {
      uri = getVersionedAPIPath(String.format("apps/%s/%s", appName, programType),
                                Constants.Gateway.API_VERSION_3_TOKEN, namespace);
    }
    return doGet(uri);
  }

  private void verifyInitialBatchStatusOutput(HttpResponse response) throws IOException {
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    List<JsonObject> returnedBody = readResponse(response, LIST_OF_JSONOBJECT_TYPE);
    for (JsonObject obj : returnedBody) {
      Assert.assertEquals(200, obj.get("statusCode").getAsInt());
      Assert.assertEquals(ProgramController.State.STOPPED.toString(), obj.get("status").getAsString());
    }
  }

  private void verifyInitialBatchInstanceOutput(HttpResponse response) throws IOException {
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    List<JsonObject> returnedBody = readResponse(response, LIST_OF_JSONOBJECT_TYPE);
    for (JsonObject obj : returnedBody) {
      Assert.assertEquals(200, obj.get("statusCode").getAsInt());
      Assert.assertEquals(1, obj.get("requested").getAsInt());
      Assert.assertEquals(0, obj.get("provisioned").getAsInt());
    }
  }

  private String getRunnableStatus(String namespaceId, String appId, String runnableType, String runnableId)
    throws Exception {
    HttpResponse response = doGet(getVersionedAPIPath("apps/" + appId + "/" + runnableType + "/" + runnableId +
                                                        "/status", Constants.Gateway.API_VERSION_3_TOKEN, namespaceId));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Map<String, String> o = GSON.fromJson(s, MAP_STRING_STRING_TYPE);
    return o.get("status");
  }

  private void testHistory(Class<?> app, String namespace, String appId, String runnableType, String runnableId)
    throws Exception {
    try {
      deploy(app, Constants.Gateway.API_VERSION_3_TOKEN, namespace);
      // first run
      Assert.assertEquals(200, getRunnableStartStop(namespace, appId, runnableType, runnableId, "start"));
      waitState(namespace, appId, runnableType, runnableId, ProgramRunStatus.RUNNING.toString());
      Assert.assertEquals(200, getRunnableStartStop(namespace, appId, runnableType, runnableId, "stop"));
      waitState(namespace, appId, runnableType, runnableId, ProgramController.State.STOPPED.toString());

      // second run
      Assert.assertEquals(200, getRunnableStartStop(namespace, appId, runnableType, runnableId, "start"));
      waitState(namespace, appId, runnableType, runnableId, ProgramRunStatus.RUNNING.toString());
      String url = String.format("apps/%s/%s/%s/runs?status=running", appId, runnableType, runnableId);

      //active size should be 1
      historyStatusWithRetry(getVersionedAPIPath(url, Constants.Gateway.API_VERSION_3_TOKEN, namespace), 1);
      // completed runs size should be 1
      url = String.format("apps/%s/%s/%s/runs?status=completed", appId, runnableType, runnableId);
      historyStatusWithRetry(getVersionedAPIPath(url, Constants.Gateway.API_VERSION_3_TOKEN, namespace), 1);

      Assert.assertEquals(200, getRunnableStartStop(namespace, appId, runnableType, runnableId, "stop"));
      waitState(namespace, appId, runnableType, runnableId, ProgramController.State.STOPPED.toString());

      historyStatusWithRetry(getVersionedAPIPath(url, Constants.Gateway.API_VERSION_3_TOKEN, namespace), 2);

    } finally {
      Assert.assertEquals(200, doDelete(getVersionedAPIPath("apps/" + appId, Constants.Gateway.API_VERSION_3_TOKEN,
                                                            namespace)).getStatusLine().getStatusCode());
    }
  }

  private void historyStatusWithRetry(String url, int size) throws Exception {
    int trials = 0;
    while (trials++ < 5) {
      HttpResponse response = doGet(url);
      List<Map<String, String>> result = GSON.fromJson(EntityUtils.toString(response.getEntity()),
                                                       new TypeToken<List<Map<String, String>>>() {
                                                       }.getType());

      if (result.size() >= size) {
        // For each one, we have 4 fields.
        for (Map<String, String> m : result) {
          int expectedFieldSize = m.get("status").equals("RUNNING") ? 3 : 4;
          Assert.assertEquals(expectedFieldSize, m.size());
        }
        break;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertTrue(trials < 5);
  }

  private void testRuntimeArgs(Class<?> app, String namespace, String appId, String runnableType, String runnableId)
    throws Exception {
    deploy(app, Constants.Gateway.API_VERSION_3_TOKEN, namespace);

    Map<String, String> args = Maps.newHashMap();
    args.put("Key1", "Val1");
    args.put("Key2", "Val1");
    args.put("Key2", "Val1");

    HttpResponse response;
    String argString = GSON.toJson(args, new TypeToken<Map<String, String>>() {
    }.getType());
    String versionedRuntimeArgsUrl = getVersionedAPIPath("apps/" + appId + "/" + runnableType + "/" + runnableId +
                                                           "/runtimeargs", Constants.Gateway.API_VERSION_3_TOKEN,
                                                         namespace);
    response = doPut(versionedRuntimeArgsUrl, argString);

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doGet(versionedRuntimeArgsUrl);

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Map<String, String> argsRead = GSON.fromJson(EntityUtils.toString(response.getEntity()),
                                                 new TypeToken<Map<String, String>>() {
                                                 }.getType());

    Assert.assertEquals(args.size(), argsRead.size());

    for (Map.Entry<String, String> entry : args.entrySet()) {
      Assert.assertEquals(entry.getValue(), argsRead.get(entry.getKey()));
    }

    //test empty runtime args
    response = doPut(versionedRuntimeArgsUrl, "");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = doGet(versionedRuntimeArgsUrl);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    argsRead = GSON.fromJson(EntityUtils.toString(response.getEntity()),
                             new TypeToken<Map<String, String>>() {
                             }.getType());
    Assert.assertEquals(0, argsRead.size());

    //test null runtime args
    response = doPut(versionedRuntimeArgsUrl, null);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = doGet(versionedRuntimeArgsUrl);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    argsRead = GSON.fromJson(EntityUtils.toString(response.getEntity()),
                             new TypeToken<Map<String, String>>() {
                             }.getType());
    Assert.assertEquals(0, argsRead.size());
  }
}
