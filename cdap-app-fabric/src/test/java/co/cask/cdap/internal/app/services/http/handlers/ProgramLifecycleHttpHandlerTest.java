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

package co.cask.cdap.internal.app.services.http.handlers;

import co.cask.cdap.AppWithServices;
import co.cask.cdap.DummyAppWithTrackingTable;
import co.cask.cdap.SleepingWorkflowApp;
import co.cask.cdap.WordCountApp;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.ProgramLifecycleHttpHandler;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.XSlowTests;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link ProgramLifecycleHttpHandler}
 */
public class ProgramLifecycleHttpHandlerTest extends AppFabricTestBase {

  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type LIST_OF_JSONOBJECT_TYPE = new TypeToken<List<JsonObject>>() { }.getType();

  // TODO: These should probably be defined in the base class to share with AppLifecycleHttpHandlerTest
  private static final String TEST_NAMESPACE1 = "testnamespace1";
  private static final NamespaceMeta TEST_NAMESPACE_META1 = new NamespaceMeta.Builder().setName(TEST_NAMESPACE1)
    .setDisplayName(TEST_NAMESPACE1).setDescription(TEST_NAMESPACE1).build();
  private static final String TEST_NAMESPACE2 = "testnamespace2";
  private static final NamespaceMeta TEST_NAMESPACE_META2 = new NamespaceMeta.Builder().setName(TEST_NAMESPACE2)
    .setDisplayName(TEST_NAMESPACE2).setDescription(TEST_NAMESPACE2).build();

  private static final String WORDCOUNT_APP_NAME = "WordCountApp";
  private static final String WORDCOUNT_FLOW_NAME = "WordCountFlow";
  private static final String WORDCOUNT_FLOWLET_NAME = "StreamSource";
  private static final String DUMMY_APP_ID = "dummy";
  private static final String DUMMY_RUNNABLE_ID = "dummy-batch";
  private static final String SLEEP_WORKFLOW_APP_ID = "SleepWorkflowApp";
  private static final String SLEEP_WORKFLOW_RUNNABLE_ID = "SleepWorkflow";
  private static final String APP_WITH_SERVICES_APP_ID = "AppWithServices";
  private static final String APP_WITH_SERVICES_SERVICE_NAME = "NoOpService";

  @BeforeClass
  public static void setup() throws Exception {
    HttpResponse response = doPut(String.format("%s/namespaces", Constants.Gateway.API_VERSION_3),
                                  GSON.toJson(TEST_NAMESPACE_META1));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = doPut(String.format("%s/namespaces", Constants.Gateway.API_VERSION_3),
                     GSON.toJson(TEST_NAMESPACE_META2));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Category(XSlowTests.class)
  @Test
  public void testRunnableStatus() throws Exception {

    //deploy and check the status
    deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    //check the status of the deployment
    Assert.assertEquals("DEPLOYED", getDeploymentStatus());
    Assert.assertEquals(ProgramController.State.STOPPED.toString(), getRunnableStatus(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(), WORDCOUNT_FLOW_NAME));

    //start flow and check the status
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(), WORDCOUNT_FLOW_NAME, "start"));
    waitState(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(), WORDCOUNT_FLOW_NAME, ProgramRunStatus.RUNNING.toString());

    //stop the flow and check the status
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(), WORDCOUNT_FLOW_NAME, "stop"));
    waitState(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(), WORDCOUNT_FLOW_NAME, ProgramController.State.STOPPED.toString());

    deploy(DummyAppWithTrackingTable.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);
    Assert.assertEquals("DEPLOYED", getDeploymentStatus());

    //start map-reduce and check status and stop the map-reduce job and check the status ..
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE2, DUMMY_APP_ID, ProgramType.MAPREDUCE.getCategoryName(), DUMMY_RUNNABLE_ID, "start"));
    waitState(TEST_NAMESPACE2, DUMMY_APP_ID, ProgramType.MAPREDUCE.getCategoryName(), DUMMY_RUNNABLE_ID, ProgramRunStatus.RUNNING.toString());

    //stop the mapreduce program and check the status
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE2, DUMMY_APP_ID, ProgramType.MAPREDUCE.getCategoryName(), DUMMY_RUNNABLE_ID, "stop"));
    waitState(TEST_NAMESPACE2, DUMMY_APP_ID, ProgramType.MAPREDUCE.getCategoryName(), DUMMY_RUNNABLE_ID, ProgramController.State.STOPPED.toString());

    // cleanup
    HttpResponse response = doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Category(XSlowTests.class)
  @Test
  public void testStartStop() throws Exception {
    //deploy, check the status and start a flow. Also check the status
    deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals("STOPPED", getRunnableStatus(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(), WORDCOUNT_FLOW_NAME));
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(), WORDCOUNT_FLOW_NAME, "start"));
    waitState(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(), WORDCOUNT_FLOW_NAME, ProgramRunStatus.RUNNING.toString());

    //web-app, start, stop and status check - enable after webapp apis are implemented.
//    Assert.assertEquals(200, doPost("/v2/apps/" + WORDCOUNT_APP_NAME + "/webapp/start", null).getStatusLine().getStatusCode());
//
//    Assert.assertEquals(ProgramRunStatus.RUNNING.toString(), getWebappStatus(WORDCOUNT_APP_NAME));
//    Assert.assertEquals(200, doPost("/v2/apps/" + WORDCOUNT_APP_NAME + "/webapp/stop", null).getStatusLine().getStatusCode());
//    Assert.assertEquals("STOPPED", getWebappStatus(WORDCOUNT_APP_NAME));

    // Stop the flow and check its status
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(), WORDCOUNT_FLOW_NAME, "stop"));
    waitState(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(), WORDCOUNT_FLOW_NAME, ProgramController.State.STOPPED.toString());

    //start map-reduce and check status and stop the map-reduce job and check the status ..
    deploy(DummyAppWithTrackingTable.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE2, DUMMY_APP_ID, ProgramType.MAPREDUCE.getCategoryName(), DUMMY_RUNNABLE_ID, "start"));
    waitState(TEST_NAMESPACE2, DUMMY_APP_ID, ProgramType.MAPREDUCE.getCategoryName(), DUMMY_RUNNABLE_ID, ProgramRunStatus.RUNNING.toString());
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE2, DUMMY_APP_ID, ProgramType.MAPREDUCE.getCategoryName(), DUMMY_RUNNABLE_ID, "stop"));
    waitState(TEST_NAMESPACE2, DUMMY_APP_ID, ProgramType.MAPREDUCE.getCategoryName(), DUMMY_RUNNABLE_ID, ProgramController.State.STOPPED.toString());

    //deploy and check status of a workflow
    deploy(SleepingWorkflowApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE2, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW.getCategoryName(), SLEEP_WORKFLOW_RUNNABLE_ID, "start"));
    waitState(TEST_NAMESPACE2, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW.getCategoryName(), SLEEP_WORKFLOW_RUNNABLE_ID, ProgramRunStatus.RUNNING.toString());
    // workflow will stop itself
    waitState(TEST_NAMESPACE2, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW.getCategoryName(), SLEEP_WORKFLOW_RUNNABLE_ID, ProgramController.State.STOPPED.toString());

    // removing apps
    Assert.assertEquals(200, doDelete(getVersionedAPIPath("apps/" + WORDCOUNT_APP_NAME, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1)).getStatusLine().getStatusCode());
    Assert.assertEquals(200, doDelete(getVersionedAPIPath("apps/" + DUMMY_APP_ID, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2)).getStatusLine().getStatusCode());
    Assert.assertEquals(200, doDelete(getVersionedAPIPath("apps/" + SLEEP_WORKFLOW_APP_ID, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2)).getStatusLine().getStatusCode());
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
      Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE1, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW.getCategoryName(), SLEEP_WORKFLOW_RUNNABLE_ID, "start"));
      waitState(TEST_NAMESPACE1, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW.getCategoryName(), SLEEP_WORKFLOW_RUNNABLE_ID, ProgramRunStatus.RUNNING.toString());
      // workflow stops by itself after actions are done
      waitState(TEST_NAMESPACE1, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW.getCategoryName(), SLEEP_WORKFLOW_RUNNABLE_ID, ProgramController.State.STOPPED.toString());

      // second run
      Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE1, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW.getCategoryName(), SLEEP_WORKFLOW_RUNNABLE_ID, "start"));
      waitState(TEST_NAMESPACE1, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW.getCategoryName(), SLEEP_WORKFLOW_RUNNABLE_ID, ProgramRunStatus.RUNNING.toString());
      // workflow stops by itself after actions are done
      waitState(TEST_NAMESPACE1, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW.getCategoryName(), SLEEP_WORKFLOW_RUNNABLE_ID, ProgramController.State.STOPPED.toString());

      String url = String.format("apps/%s/%s/%s/runs?status=completed", SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW.getCategoryName(), SLEEP_WORKFLOW_RUNNABLE_ID);
      historyStatusWithRetry(getVersionedAPIPath(url, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1), 2);

    } finally {
      Assert.assertEquals(200, doDelete(getVersionedAPIPath("apps/" + SLEEP_WORKFLOW_APP_ID, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1)).getStatusLine().getStatusCode());
    }
  }

  @Test
  public void testFlowRuntimeArgs() throws Exception {
    testRuntimeArgs(WordCountApp.class, TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(), WORDCOUNT_FLOW_NAME);
  }

  @Test
  public void testWorkflowRuntimeArgs() throws Exception {
    testRuntimeArgs(SleepingWorkflowApp.class, TEST_NAMESPACE2, SLEEP_WORKFLOW_APP_ID, ProgramType.WORKFLOW.getCategoryName(), SLEEP_WORKFLOW_RUNNABLE_ID);
  }

  @Test
  public void testMapreduceRuntimeArgs() throws Exception {
    testRuntimeArgs(DummyAppWithTrackingTable.class, TEST_NAMESPACE1, DUMMY_APP_ID, ProgramType.MAPREDUCE.getCategoryName(), DUMMY_RUNNABLE_ID);
  }

  @Test
  public void testBatchStatus() throws Exception {
    final String statusUrl1 = getVersionedAPIPath("status", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    final String statusUrl2 = getVersionedAPIPath("status", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);

    // invalid json must return 400
    Assert.assertEquals(400, doPost(statusUrl1, "").getStatusLine().getStatusCode());
    Assert.assertEquals(400, doPost(statusUrl2, "").getStatusLine().getStatusCode());
    // empty array is valid args
    Assert.assertEquals(200, doPost(statusUrl1, "[]").getStatusLine().getStatusCode());
    Assert.assertEquals(200, doPost(statusUrl2, "[]").getStatusLine().getStatusCode());

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
                        doPost(statusUrl1, "[{'appId':'WordCountApp', 'programType':'Flow' 'programId':'WordCountFlow'}]").getStatusLine().getStatusCode());
    // Test missing app, programType, etc
    List<JsonObject> returnedBody = readResponse(doPost(statusUrl1, "[{'appId':'NotExist', 'programType':'Flow', 'programId':'WordCountFlow'}]"), LIST_OF_JSONOBJECT_TYPE);
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
                                     "{'appId': 'WordCountApp', 'programType': 'Procedure', 'programId': 'WordFrequency'}]");
    verifyInitialBatchStatusOutput(response);

    // test valid cases for namespace2
    response = doPost(statusUrl2, "[{'appId': 'AppWithServices', 'programType': 'Service', 'programId': 'NoOpService'}]");
    verifyInitialBatchStatusOutput(response);


    // start the flow
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(), WORDCOUNT_FLOW_NAME, "start"));
    // test status API after starting the flow
    response = doPost(statusUrl1, "[{'appId':'WordCountApp', 'programType':'Flow', 'programId':'WordCountFlow'}," +
      "{'appId': 'WordCountApp', 'programType': 'Mapreduce', 'programId': 'VoidMapReduceJob'}]");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    returnedBody = readResponse(response, LIST_OF_JSONOBJECT_TYPE);
    Assert.assertEquals(ProgramRunStatus.RUNNING.toString(), returnedBody.get(0).get("status").getAsString());
    Assert.assertEquals(ProgramController.State.STOPPED.toString(), returnedBody.get(1).get("status").getAsString());

    // start the service
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE2, APP_WITH_SERVICES_APP_ID, ProgramType.SERVICE.getCategoryName(), APP_WITH_SERVICES_SERVICE_NAME, "start"));
    // test status API after starting the service
    response = doPost(statusUrl2, "[{'appId': 'AppWithServices', 'programType': 'Service', 'programId': 'NoOpService'}]");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    returnedBody = readResponse(response, LIST_OF_JSONOBJECT_TYPE);
    Assert.assertEquals(ProgramRunStatus.RUNNING.toString(), returnedBody.get(0).get("status").getAsString());

    // stop the flow
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(), WORDCOUNT_FLOW_NAME, "stop"));
    waitState(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(), WORDCOUNT_FLOW_NAME, ProgramController.State.STOPPED.toString());
    // stop the service
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE2, APP_WITH_SERVICES_APP_ID, ProgramType.SERVICE.getCategoryName(), APP_WITH_SERVICES_SERVICE_NAME, "stop"));
    waitState(TEST_NAMESPACE2, APP_WITH_SERVICES_APP_ID, ProgramType.SERVICE.getCategoryName(), APP_WITH_SERVICES_SERVICE_NAME, ProgramController.State.STOPPED.toString());


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
    final String instancesUrl1 = getVersionedAPIPath("instances", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    final String instancesUrl2 = getVersionedAPIPath("instances", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);

    Assert.assertEquals(400, doPost(instancesUrl1, "").getStatusLine().getStatusCode());
    Assert.assertEquals(400, doPost(instancesUrl2, "").getStatusLine().getStatusCode());

    // empty array is valid args
    Assert.assertEquals(200, doPost(instancesUrl1, "[]").getStatusLine().getStatusCode());
    Assert.assertEquals(200, doPost(instancesUrl2, "[]").getStatusLine().getStatusCode());

    deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    deploy(AppWithServices.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2);

    // data requires appId, programId, and programType. Test missing fields/invalid programType
    // TODO: These json strings should be replaced with static inner classes so it becomes easier to refactor in future
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
      doPost(instancesUrl1, "[{'appId':'NotExist', 'programType':'Flow', 'programId':'WordCountFlow'}]"), LIST_OF_JSONOBJECT_TYPE);
    Assert.assertEquals(404, returnedBody.get(0).get("statusCode").getAsInt());
    returnedBody = readResponse(
      doPost(instancesUrl1, "[{'appId':'WordCountApp', 'programType':'flow', 'programId':'WordCountFlow', 'runnableId': " +
        "NotExist'}]"), LIST_OF_JSONOBJECT_TYPE);
    Assert.assertEquals(404, returnedBody.get(0).get("statusCode").getAsInt());


    // valid test in namespace1
    HttpResponse response = doPost(instancesUrl1,
                                   "[{'appId':'WordCountApp', 'programType':'Flow', 'programId':'WordCountFlow', 'runnableId': 'StreamSource'}," +
                                     "{'appId': 'WordCountApp', 'programType': 'Procedure', 'programId': 'WordFrequency'}]");

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
    Assert.assertEquals(200, getRunnableStartStop(TEST_NAMESPACE2, APP_WITH_SERVICES_APP_ID, ProgramType.SERVICE.getCategoryName(), APP_WITH_SERVICES_SERVICE_NAME, "start"));
    response = doPost(instancesUrl2, "[{'appId':'AppWithServices', 'programType':'Service','programId':'NoOpService', 'runnableId':'NoOpService'}]");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    returnedBody = readResponse(response, LIST_OF_JSONOBJECT_TYPE);
    Assert.assertEquals(1, returnedBody.get(0).get("provisioned").getAsInt());

    // request for more instances of the flowlet
    // TODO: verify this when the flowlets/instances v3 API is implemented
//    String flowletInstancesUrl = String.format("apps/%s/%s/%s/flowlets/%s/instances", WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(), WORDCOUNT_FLOW_NAME, WORDCOUNT_FLOWLET_NAME);
//    String flowletInstancesVersionedUrl = getVersionedAPIPath(flowletInstancesUrl, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
//    Assert.assertEquals(200, doPut(flowletInstancesVersionedUrl, "{'instances': 2}").getStatusLine().getStatusCode());
//    returnedBody = readResponse(doPost(instancesUrl1, "[{'appId':'WordCountApp', 'programType':'Flow'," +
//      "'programId':'WordCountFlow', 'runnableId': 'StreamSource'}]"), LIST_OF_JSONOBJECT_TYPE);
//    // verify that 2 more instances were requested
//    Assert.assertEquals(2, returnedBody.get(0).get("requested").getAsInt());


    getRunnableStartStop(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(),
                         WORDCOUNT_FLOW_NAME, "stop");
    getRunnableStartStop(TEST_NAMESPACE2, APP_WITH_SERVICES_APP_ID, ProgramType.SERVICE.getCategoryName(), APP_WITH_SERVICES_SERVICE_NAME, "stop");

    waitState(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW.getCategoryName(), WORDCOUNT_FLOW_NAME, ProgramController.State.STOPPED.toString());
    waitState(TEST_NAMESPACE2, APP_WITH_SERVICES_APP_ID, ProgramType.SERVICE.getCategoryName(), APP_WITH_SERVICES_SERVICE_NAME, ProgramController.State.STOPPED.toString());
  }

  @After
  public void cleanup() throws Exception {
    doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    HttpResponse response = doDelete(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3,
                                                   TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doDelete(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, TEST_NAMESPACE2));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
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

  //TODO: Should move to base class
  private String getDeploymentStatus() throws Exception {
    HttpResponse response = doGet(getVersionedAPIPath("deploy/status/", Constants.Gateway.API_VERSION_3_TOKEN,
                                                      TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Map<String, String> o = new Gson().fromJson(s, MAP_STRING_STRING_TYPE);
    return o.get("status");
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

  private int getRunnableStartStop(String namespaceId, String appId, String runnableType, String runnableId,
                                   String action) throws Exception {
    HttpResponse response = doPost(getVersionedAPIPath("apps/" + appId + "/" + runnableType + "/" + runnableId + "/" +
                                                         action, Constants.Gateway.API_VERSION_3_TOKEN, namespaceId));
    return response.getStatusLine().getStatusCode();
  }

  private void waitState(String namespaceId, String appId, String runnableType, String runnableId, String state)
    throws Exception {
    int trials = 0;
    // it may take a while for workflow/mr to start...
    int maxTrials = 120;
    while (trials++ < maxTrials) {
      HttpResponse response =
        doGet(getVersionedAPIPath(String.format("apps/%s/%s/%s/status", appId, runnableType, runnableId),
                                  Constants.Gateway.API_VERSION_3_TOKEN, namespaceId));
      JsonObject status = GSON.fromJson(EntityUtils.toString(response.getEntity()), JsonObject.class);
      if (status != null && status.has("status") && state.equals(status.get("status").getAsString())) {
        break;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertTrue(trials < maxTrials);
  }

  private void testHistory(Class<?> app, String namespace, String appId, String runnableType, String runnableId)
    throws Exception {
    try {
      deploy(app, Constants.Gateway.API_VERSION_3_TOKEN, namespace);
      // first run
      Assert.assertEquals(200, getRunnableStartStop(namespace, appId, runnableType, runnableId, "start"));
      waitState(namespace, appId, runnableType, runnableId, "RUNNING");
      Assert.assertEquals(200, getRunnableStartStop(namespace, appId, runnableType, runnableId, "stop"));
      waitState(namespace, appId, runnableType, runnableId, "STOPPED");

      // second run
      Assert.assertEquals(200, getRunnableStartStop(namespace, appId, runnableType, runnableId, "start"));
      waitState(namespace, appId, runnableType, runnableId, "RUNNING");
      String url = String.format("apps/%s/%s/%s/runs?status=running", appId, runnableType, runnableId);

      //active size should be 1
      historyStatusWithRetry(getVersionedAPIPath(url, Constants.Gateway.API_VERSION_3_TOKEN, namespace), 1);
      // completed runs size should be 1
      url = String.format("apps/%s/%s/%s/runs?status=completed", appId, runnableType, runnableId);
      historyStatusWithRetry(getVersionedAPIPath(url, Constants.Gateway.API_VERSION_3_TOKEN, namespace), 1);

      Assert.assertEquals(200, getRunnableStartStop(namespace, appId, runnableType, runnableId, "stop"));
      waitState(namespace, appId, runnableType, runnableId, "STOPPED");

      historyStatusWithRetry(getVersionedAPIPath(url, Constants.Gateway.API_VERSION_3_TOKEN, namespace), 2);

    } finally {
      Assert.assertEquals(200, doDelete(getVersionedAPIPath("apps/" + appId, Constants.Gateway.API_VERSION_3_TOKEN, namespace)).getStatusLine().getStatusCode());
    }
  }

  private void historyStatusWithRetry(String url, int size) throws Exception {
    int trials = 0;
    while (trials++ < 5) {
      HttpResponse response = doGet(url);
      List<Map<String, String>> result = GSON.fromJson(EntityUtils.toString(response.getEntity()),
                                                       new TypeToken<List<Map<String, String>>>() { }.getType());

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
    String argString = GSON.toJson(args, new TypeToken<Map<String, String>>() { }.getType());
    String versionedRuntimeArgsUrl = getVersionedAPIPath("apps/" + appId + "/" + runnableType + "/" + runnableId + "/runtimeargs", Constants.Gateway.API_VERSION_3_TOKEN, namespace);
    response = doPut(versionedRuntimeArgsUrl, argString);

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doGet(versionedRuntimeArgsUrl);

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Map<String, String> argsRead = GSON.fromJson(EntityUtils.toString(response.getEntity()),
                                                 new TypeToken<Map<String, String>>() { }.getType());

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
                             new TypeToken<Map<String, String>>() { }.getType());
    Assert.assertEquals(0, argsRead.size());

    //test null runtime args
    response = doPut(versionedRuntimeArgsUrl, null);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = doGet(versionedRuntimeArgsUrl);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    argsRead = GSON.fromJson(EntityUtils.toString(response.getEntity()),
                             new TypeToken<Map<String, String>>() { }.getType());
    Assert.assertEquals(0, argsRead.size());
  }

  /**
   * Enable after implementing v3 webapps
   */
  /*private String getWebappStatus(String appId) throws Exception {
    HttpResponse response = doGet("/v2/apps/" + appId + "/" + "webapp" + "/status");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Map<String, String> o = new Gson().fromJson(s, MAP_STRING_STRING_TYPE);
    return o.get("status");
  }*/







  /**
   * Metadata tests through appfabric apis
   */
  /*@Test
  public void testGetMetadata() throws Exception {
    try {
      HttpResponse response = doPost("/v2/unrecoverable/reset");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());

      response = deploy(WordCountApp.class);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());

      response = deploy(AppWithWorkflow.class);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());

      response = doGet("/v2/apps/WordCountApp/flows/WordCountFlow");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      String result = EntityUtils.toString(response.getEntity());
      Assert.assertNotNull(result);
      Assert.assertTrue(result.contains("WordCountFlow"));

      // verify procedure
      response = doGet("/v2/apps/WordCountApp/procedures/WordFrequency");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      result = EntityUtils.toString(response.getEntity());
      Assert.assertNotNull(result);
      Assert.assertTrue(result.contains("WordFrequency"));

      //verify mapreduce
      response = doGet("/v2/apps/WordCountApp/mapreduce/VoidMapReduceJob");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      result = EntityUtils.toString(response.getEntity());
      Assert.assertNotNull(result);
      Assert.assertTrue(result.contains("VoidMapReduceJob"));

      // verify single workflow
      response = doGet("/v2/apps/AppWithWorkflow/workflows/SampleWorkflow");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      result = EntityUtils.toString(response.getEntity());
      Assert.assertNotNull(result);
      Assert.assertTrue(result.contains("SampleWorkflow"));

      // verify apps
      response = doGet("/v2/apps");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      String s = EntityUtils.toString(response.getEntity());
      List<Map<String, String>> o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(2, o.size());
      Assert.assertTrue(o.contains(ImmutableMap.of("type", "App", "id", "WordCountApp", "name", "WordCountApp",
                                                   "description", "Application for counting words")));
      Assert.assertTrue(o.contains(ImmutableMap.of("type", "App", "id", "AppWithWorkflow", "name",
                                                   "AppWithWorkflow", "description", "Sample application")));

      // verify a single app
      response = doGet("/v2/apps/WordCountApp");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      Map<String, String> app = new Gson().fromJson(s, MAP_STRING_STRING_TYPE);
      Assert.assertEquals(ImmutableMap.of("type", "App", "id", "WordCountApp", "name", "WordCountApp",
                                          "description", "Application for counting words"), app);

      // verify flows
      response = doGet("/v2/flows");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(1, o.size());
      Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WordCountApp", "id", "WordCountFlow", "name",
                                                   "WordCountFlow", "description", "Flow for counting words")));

      // verify flows by app
      response = doGet("/v2/apps/WordCountApp/flows");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(1, o.size());
      Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WordCountApp", "id", "WordCountFlow", "name",
                                                   "WordCountFlow", "description", "Flow for counting words")));

      // verify procedures
      response = doGet("/v2/procedures");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(1, o.size());
      Assert.assertTrue(o.contains(ImmutableMap.of("type", "Procedure", "app", "WordCountApp", "id", "WordFrequency",
                                                   "name", "WordFrequency", "description",
                                                   "Procedure for executing WordFrequency.")));

      // verify procedures by app
      response = doGet("/v2/apps/WordCountApp/procedures");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(1, o.size());
      Assert.assertTrue(o.contains(ImmutableMap.of("type", "Procedure", "app", "WordCountApp", "id", "WordFrequency",
                                                   "name", "WordFrequency", "description",
                                                   "Procedure for executing WordFrequency.")));


      // verify mapreduces
      response = doGet("/v2/mapreduce");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(1, o.size());
      Assert.assertTrue(o.contains(ImmutableMap.of("type", "Mapreduce", "app", "WordCountApp", "id", "VoidMapReduceJob",
                                                   "name", "VoidMapReduceJob",
                                                   "description", "Mapreduce that does nothing " +
          "(and actually doesn't run) - it is here for testing MDS")));

      // verify workflows
      response = doGet("/v2/workflows");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(1, o.size());
      Assert.assertTrue(o.contains(ImmutableMap.of(
        "type", "Workflow", "app", "AppWithWorkflow", "id", "SampleWorkflow",
        "name", "SampleWorkflow", "description",  "SampleWorkflow description")));


      // verify programs by non-existent app
      response = doGet("/v2/apps/NonExistenyApp/flows");
      Assert.assertEquals(404, response.getStatusLine().getStatusCode());
      response = doGet("/v2/apps/NonExistenyApp/procedures");
      Assert.assertEquals(404, response.getStatusLine().getStatusCode());
      response = doGet("/v2/apps/NonExistenyApp/mapreduce");
      Assert.assertEquals(404, response.getStatusLine().getStatusCode());
      response = doGet("/v2/apps/NonExistenyApp/workflows");
      Assert.assertEquals(404, response.getStatusLine().getStatusCode());

      // verify programs by app that does not have that program type
      response = doGet("/v2/apps/AppWithWorkflow/flows");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertTrue(o.isEmpty());
      response = doGet("/v2/apps/AppWithWorkflow/procedures");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertTrue(o.isEmpty());
      response = doGet("/v2/apps/AppWithWorkflow/mapreduce");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertTrue(o.isEmpty());
      response = doGet("/v2/apps/WordCountApp/workflows");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertTrue(o.isEmpty());

      // verify flows by stream
      response = doGet("/v2/streams/text/flows");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(1, o.size());
      Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WordCountApp", "id", "WordCountFlow", "name",
                                                   "WordCountFlow", "description", "Flow for counting words")));

      // verify flows by dataset
      response = doGet("/v2/datasets/mydataset/flows");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(1, o.size());
      Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WordCountApp", "id", "WordCountFlow", "name",
                                                   "WordCountFlow", "description", "Flow for counting words")));

      // verify one dataset
      response = doGet("/v2/datasets/mydataset");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      Map<String, String> map = new Gson().fromJson(s, MAP_STRING_STRING_TYPE);
      Assert.assertNotNull(map);
      Assert.assertEquals("mydataset", map.get("id"));
      Assert.assertEquals("mydataset", map.get("name"));

      // verify all datasets
      response = doGet("/v2/datasets");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(3, o.size());
      Map<String, String> expectedDataSets = ImmutableMap.<String, String>builder()
        .put("input", ObjectStore.class.getName())
        .put("output", ObjectStore.class.getName())
        .put("mydataset", KeyValueTable.class.getName()).build();
      for (Map<String, String> ds : o) {
        Assert.assertTrue("problem with dataset " + ds.get("id"), ds.containsKey("id"));
        Assert.assertTrue("problem with dataset " + ds.get("id"), ds.containsKey("name"));
        Assert.assertTrue("problem with dataset " + ds.get("id"), ds.containsKey("classname"));
        Assert.assertTrue("problem with dataset " + ds.get("id"), expectedDataSets.containsKey(ds.get("id")));
        Assert.assertEquals("problem with dataset " + ds.get("id"),
                            expectedDataSets.get(ds.get("id")), ds.get("classname"));
      }

      // verify datasets by app
      response = doGet("/v2/apps/WordCountApp/datasets");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(1, o.size());
      expectedDataSets = ImmutableMap.<String, String>builder()
        .put("mydataset", KeyValueTable.class.getName()).build();
      for (Map<String, String> ds : o) {
        Assert.assertTrue("problem with dataset " + ds.get("id"), ds.containsKey("id"));
        Assert.assertTrue("problem with dataset " + ds.get("id"), ds.containsKey("name"));
        Assert.assertTrue("problem with dataset " + ds.get("id"), ds.containsKey("classname"));
        Assert.assertTrue("problem with dataset " + ds.get("id"), expectedDataSets.containsKey(ds.get("id")));
        Assert.assertEquals("problem with dataset " + ds.get("id"),
                            expectedDataSets.get(ds.get("id")), ds.get("classname"));
      }

      // verify one stream
      response = doGet("/v2/streams/text");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      map = new Gson().fromJson(s, MAP_STRING_STRING_TYPE);
      Assert.assertNotNull(map);
      Assert.assertEquals("text", map.get("id"));
      Assert.assertEquals("text", map.get("name"));
      Assert.assertNotNull(map.get("specification"));
      StreamSpecification sspec = new Gson().fromJson(map.get("specification"), StreamSpecification.class);
      Assert.assertNotNull(sspec);

      // verify all streams
      response = doGet("/v2/streams");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(1, o.size());
      Set<String> expectedStreams = ImmutableSet.of("text");
      for (Map<String, String> stream : o) {
        Assert.assertTrue("problem with stream " + stream.get("id"), stream.containsKey("id"));
        Assert.assertTrue("problem with stream " + stream.get("id"), stream.containsKey("name"));
        Assert.assertTrue("problem with dataset " + stream.get("id"), expectedStreams.contains(stream.get("id")));
      }

      // verify streams by app
      response = doGet("/v2/apps/WordCountApp/streams");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(1, o.size());
      expectedStreams = ImmutableSet.of("text");
      for (Map<String, String> stream : o) {
        Assert.assertTrue("problem with stream " + stream.get("id"), stream.containsKey("id"));
        Assert.assertTrue("problem with stream " + stream.get("id"), stream.containsKey("name"));
        Assert.assertTrue("problem with dataset " + stream.get("id"), expectedStreams.contains(stream.get("id")));
      }
    } finally {
      Assert.assertEquals(200, doDelete("/v2/apps").getStatusLine().getStatusCode());
    }
  }*/


  /**
   * Tests for program list calls
   */
  /*@Test
  public void testProgramList() throws Exception {
    //Test :: /flows /procedures /mapreduce /workflows
    //App  :: /apps/AppName/flows /procedures /mapreduce /workflows
    //App Info :: /apps/AppName
    //All Apps :: /apps

    HttpResponse response = doGet("/v2/flows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doGet("/v2/procedures");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doGet("/v2/mapreduce");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doGet("/v2/workflows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    deploy(WordCountApp.class);
    deploy(DummyAppWithTrackingTable.class);
    response = doGet("/v2/apps/WordCountApp/flows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String json = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> flows = new Gson().fromJson(json, LIST_MAP_STRING_STRING_TYPE);
    Assert.assertEquals(1, flows.size());

    response = doGet("/v2/apps/WordCountApp/procedures");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    json = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> procedures = new Gson().fromJson(json, LIST_MAP_STRING_STRING_TYPE);
    Assert.assertEquals(1, procedures.size());

    response = doGet("/v2/apps/WordCountApp/mapreduce");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    json = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> mapreduce = new Gson().fromJson(json, LIST_MAP_STRING_STRING_TYPE);
    Assert.assertEquals(1, mapreduce.size());

    response = doGet("/v2/apps/WordCountApp/workflows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doGet("/v2/apps");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doDelete("/v2/apps/dummy");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }*/

  /**
   * Test for schedule handlers.
   */
  /*@Category(XSlowTests.class)
  @Test
  public void testScheduleEndPoints() throws Exception {
    // Steps for the test:
    // 1. Deploy the app
    // 2. Verify the schedules
    // 3. Verify the history after waiting a while
    // 4. Suspend the schedule
    // 5. Verify there are no runs after the suspend by looking at the history
    // 6. Resume the schedule
    // 7. Verify there are runs after the resume by looking at the history
    HttpResponse response = deploy(AppWithSchedule.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = doGet("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/schedules");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String json = EntityUtils.toString(response.getEntity());
    List<String> schedules = new Gson().fromJson(json, new TypeToken<List<String>>() { }.getType());
    Assert.assertEquals(1, schedules.size());
    String scheduleId = schedules.get(0);
    Assert.assertNotNull(scheduleId);
    Assert.assertFalse(scheduleId.isEmpty());

    scheduleHistoryCheck(5, "/v2/apps/AppWithSchedule/workflows/SampleWorkflow/runs?status=completed", 0);

    //Check suspend status
    String scheduleStatus = String.format("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/schedules/%s/status",
                                          scheduleId);
    scheduleStatusCheck(5, scheduleStatus, "SCHEDULED");

    String scheduleSuspend = String.format("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/schedules/%s/suspend",
                                           scheduleId);

    response = doPost(scheduleSuspend);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    //check paused state
    scheduleStatusCheck(5, scheduleStatus, "SUSPENDED");

    TimeUnit.SECONDS.sleep(2); //wait till any running jobs just before suspend call completes.

    response = doGet("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/runs?status=completed");
    json = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> history = new Gson().fromJson(json,
                                                            LIST_MAP_STRING_STRING_TYPE);
    int workflowRuns = history.size();

    //Sleep for some time and verify there are no more scheduled jobs after the suspend.
    TimeUnit.SECONDS.sleep(10);

    response = doGet("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/runs?status=completed");
    json = EntityUtils.toString(response.getEntity());
    history = new Gson().fromJson(json,
                                  LIST_MAP_STRING_STRING_TYPE);
    int workflowRunsAfterSuspend = history.size();
    Assert.assertEquals(workflowRuns, workflowRunsAfterSuspend);

    String scheduleResume = String.format("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/schedules/%s/resume",
                                          scheduleId);

    response = doPost(scheduleResume);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    scheduleHistoryCheck(5, "/v2/apps/AppWithSchedule/workflows/SampleWorkflow/runs?status=completed",
                         workflowRunsAfterSuspend);

    //check scheduled state
    scheduleStatusCheck(5, scheduleStatus, "SCHEDULED");

    //Check status of a non existing schedule
    String notFoundSchedule = String.format("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/schedules/%s/status",
                                            "invalidId");

    scheduleStatusCheck(5, notFoundSchedule, "NOT_FOUND");

    response = doPost(scheduleSuspend);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    //check paused state
    scheduleStatusCheck(5, scheduleStatus, "SUSPENDED");

    TimeUnit.SECONDS.sleep(2); //wait till any running jobs just before suspend call completes.

    response = doDelete("/v2/apps/AppWithSchedule");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }*/

  // TODO: Fix this unit test
  /*@Ignore
  @Test
  public void testClearQueuesStreams() throws Exception {
    // setup accessor
    String streamName = "doobdoobee2";
    String queueName = "doobee2";

    // create a stream, a queue, a table
    createStream(streamName);
    createQueue(queueName);

    // verify they are all there
    Assert.assertTrue(verifyStream(streamName));
    Assert.assertTrue(verifyQueue(queueName));

    // clear queues
    Assert.assertEquals(200, doDelete("/v2/queues").getStatusLine().getStatusCode());

    // verify tables and streams are still here
    Assert.assertTrue(verifyStream(streamName));
    // verify queue is gone
    Assert.assertFalse(verifyQueue(queueName));

    // recreate the queue
    createQueue(queueName);
    Assert.assertTrue(verifyQueue(queueName));

    // clear streams
    Assert.assertEquals(200, doDelete("/v2/streams").getStatusLine().getStatusCode());

    // verify table and queue are still here
    Assert.assertTrue(verifyQueue(queueName));
    // verify stream is gone
    Assert.assertFalse(verifyStream(streamName));
  }*/

  /**
   * Test for resetting app.
   */
  /*@Test
  public void testUnRecoverableReset() throws Exception {
    try {
      HttpResponse response = deploy(WordCountApp.class);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      response = doPost("/v2/unrecoverable/reset");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    } finally {
      Assert.assertEquals(200, doDelete("/v2/apps").getStatusLine().getStatusCode());
    }
    // make sure that after reset (no apps), list apps returns 200, and not 404
    Assert.assertEquals(200, doGet("/v2/apps").getStatusLine().getStatusCode());
  }*/

  /**
   * Test for resetting app.
   */
  /*@Test
  public void testUnRecoverableDatasetsDeletion() throws Exception {
    try {
      // deploy app and create datasets
      deploy(WordCountApp.class);
      getRunnableStartStop("flows", "WordCountApp", "WordCountFlow", "start");
      waitState("flows", "WordCountApp", "WordCountFlow", "RUNNING");

      // check that datasets were created
      Assert.assertTrue(
        GSON.fromJson(EntityUtils.toString(doGet("/v2/datasets").getEntity()), JsonArray.class).size() > 0);

      // try delete datasets while program is running: should fail
      HttpResponse response = doDelete("/v2/unrecoverable/data/datasets");
      Assert.assertEquals(400, response.getStatusLine().getStatusCode());

      // stop program
      getRunnableStartStop("flows", "WordCountApp", "WordCountFlow", "stop");
      waitState("flows", "WordCountApp", "WordCountFlow", "STOPPED");

      // verify delete all datasets succeeded
      response = doDelete("/v2/unrecoverable/data/datasets");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());

      // check there are no datasets
      Assert.assertEquals(
        0, GSON.fromJson(EntityUtils.toString(doGet("/v2/datasets").getEntity()), JsonArray.class).size());

    } finally {
      Assert.assertEquals(200, doDelete("/v2/apps").getStatusLine().getStatusCode());
    }
    // make sure that after reset (no apps), list apps returns 200, and not 404
    Assert.assertEquals(200, doGet("/v2/apps").getStatusLine().getStatusCode());
  }*/

  /*@Test
  public void testHistoryDeleteAfterUnrecoverableReset() throws Exception {

    String appId = "dummy";
    String runnableType = "mapreduce";
    String runnableId = "dummy-batch";

    deploy(DummyAppWithTrackingTable.class);
    // first run
    Assert.assertEquals(200, getRunnableStartStop(runnableType, appId, runnableId, "start"));
    waitState(runnableType, appId, runnableId, "RUNNING");
    Assert.assertEquals(200, getRunnableStartStop(runnableType, appId, runnableId, "stop"));
    waitState(runnableType, appId, runnableId, "STOPPED");
    String url = String.format("/v2/apps/%s/%s/%s/runs?status=completed", appId, runnableType, runnableId);
    // verify the run by checking if history has one entry
    historyStatusWithRetry(url, 1);

    HttpResponse response = doPost("/v2/unrecoverable/reset");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    // Verify that the unrecoverable reset deletes the history
    historyStatusWithRetry(url, 0);
  }*/


  /**
   * Test for resetting app.
   */
  /*@Test
  public void testUnRecoverableResetAppRunning() throws Exception {

    HttpResponse response = deploy(WordCountApp.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertEquals(200, getRunnableStartStop("flows", "WordCountApp", "WordCountFlow", "start"));
    waitState("flows", "WordCountApp", "WordCountFlow", "RUNNING");
    response = doPost("/v2/unrecoverable/reset");
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());
    Assert.assertEquals(200, getRunnableStartStop("flows", "WordCountApp", "WordCountFlow", "stop"));
  }*/

  /*@Test
  public void testServiceSpecification() throws Exception {
    deploy(AppWithServices.class);
    HttpResponse response = doGet("/v2/apps/AppWithServices/services/NoOpService/");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Set<ServiceHttpEndpoint> expectedEndpoints = ImmutableSet.of(new ServiceHttpEndpoint("GET", "/ping"),
                                                                 new ServiceHttpEndpoint("POST", "/multi"),
                                                                 new ServiceHttpEndpoint("GET", "/multi"),
                                                                 new ServiceHttpEndpoint("GET", "/multi/ping"));

    GsonBuilder gsonBuidler = new GsonBuilder();
    gsonBuidler.registerTypeAdapter(ServiceSpecification.class, new ServiceSpecificationCodec());
    gsonBuidler.registerTypeAdapter(HttpServiceHandlerSpecification.class, new HttpServiceSpecificationCodec());
    Gson gson = gsonBuidler.create();
    ServiceSpecification specification = readResponse(response, ServiceSpecification.class, gson);

    Set<ServiceHttpEndpoint> returnedEndpoints = Sets.newHashSet();
    for (HttpServiceHandlerSpecification httpServiceHandlerSpecification : specification.getHandlers().values()) {
      returnedEndpoints.addAll(httpServiceHandlerSpecification.getEndpoints());
    }

    Assert.assertEquals("NoOpService", specification.getName());
    Assert.assertTrue(returnedEndpoints.equals(expectedEndpoints));
    Assert.assertEquals(0, specification.getWorkers().values().size());
  }*/


  /*

  private void scheduleHistoryCheck(int retries, String url, int expected) throws Exception {
    int trial = 0;
    int workflowRuns = 0;
    List<Map<String, String>> history;
    String json;
    HttpResponse response;
    while (trial++ < retries) {
      response = doGet(url);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      json = EntityUtils.toString(response.getEntity());
      history = new Gson().fromJson(json, LIST_MAP_STRING_STRING_TYPE);
      workflowRuns = history.size();
      if (workflowRuns > expected) {
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertTrue(workflowRuns > expected);
  }

  private void scheduleStatusCheck(int retires, String url,
                                   String expected) throws Exception {
    int trial = 0;
    String status = null;
    String json = null;
    Map<String, String> output;
    HttpResponse response;
    while (trial++ < retires) {
      response = doGet(url);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      json = EntityUtils.toString(response.getEntity());
      output = new Gson().fromJson(json, MAP_STRING_STRING_TYPE);
      status = output.get("status");
      if (status.equals(expected)) {
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertEquals(status, expected);
  }

  */

  /*boolean verifyStream(String name) throws Exception {
    // for now, DELETE /streams only deletes the stream data, not meta data
    // boolean streamExists = 200 ==
    //   doGet("/v2/streams/" + name + "/info").getStatusLine().getStatusCode();
    return dequeueOne(QueueName.fromStream(name));
  }

  boolean verifyQueue(String name) throws Exception {
    return dequeueOne(getQueueName(name));
  }*/


  /*void createQueue(String name) throws Exception {
    // write smth to a queue
    QueueName queueName = getQueueName(name);
    enqueue(queueName, STREAM_ENTRY);
  }

  boolean dequeueOne(QueueName queueName) throws Exception {
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

  private  QueueName getQueueName(String name) {
    // i.e. flow and flowlet are constants: should be good enough
    return QueueName.fromFlowlet("app1", "flow1", "flowlet1", name);
  }*/




  /**
   * Tests taking a snapshot of the transaction manager.
   */
  /*@Test
  public void testTxManagerSnapshot() throws Exception {
    Long currentTs = System.currentTimeMillis();

    HttpResponse response = doGet("/v2/transactions/state");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    InputStream in = response.getEntity().getContent();
    SnapshotCodec snapshotCodec = getInjector().getInstance(SnapshotCodecProvider.class);
    try {
      TransactionSnapshot snapshot = snapshotCodec.decode(in);
      Assert.assertTrue(snapshot.getTimestamp() >= currentTs);
    } finally {
      in.close();
    }
  }*/

  /**
   * Tests invalidating a transaction.
   * @throws Exception
   */
  /*@Test
  public void testInvalidateTx() throws Exception {
    TransactionSystemClient txClient = AppFabricTestBase.getTxClient();

    Transaction tx1 = txClient.startShort();
    HttpResponse response = doPost("/v2/transactions/" + tx1.getWritePointer() + "/invalidate");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Transaction tx2 = txClient.startShort();
    txClient.commit(tx2);
    response = doPost("/v2/transactions/" + tx2.getWritePointer() + "/invalidate");
    Assert.assertEquals(409, response.getStatusLine().getStatusCode());

    Assert.assertEquals(400,
                        doPost("/v2/transactions/foobar/invalidate").getStatusLine().getStatusCode());
  }*/

  /*@Test
  public void testResetTxManagerState() throws Exception {
    HttpResponse response = doPost("/v2/transactions/state");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    // todo: first transaction after reset will fail, doGet() is a placeholder, can remove after tephra tx-fix
    doGet("/v2/apps");
  }*/



  /*static final QueueEntry STREAM_ENTRY = new QueueEntry("x".getBytes());

  void createStream(String name) throws Exception {
    // create stream
    Assert.assertEquals(200, doPut("/v2/streams/" + name).getStatusLine().getStatusCode());

    // write smth to a stream
    QueueName queueName = QueueName.fromStream(name);
    enqueue(queueName, STREAM_ENTRY);
  }

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
          producer.enqueue(queueEntry);
          producer.enqueue(queueEntry);
        }
      });
  }*/


  /*private int getFlowletInstances(String appId, String flowId, String flowletId) throws Exception {
    HttpResponse response =
      doGet("/v2/apps/" + appId + "/flows/" + flowId + "/flowlets/" + flowletId + "/instances");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String result = EntityUtils.toString(response.getEntity());
    Map<String, String> reply = new Gson().fromJson(result, new TypeToken<Map<String, String>>() { }.getType());
    return Integer.parseInt(reply.get("instances"));
  }

  private void setFlowletInstances(String appId, String flowId, String flowletId, int instances) throws Exception {
    JsonObject json = new JsonObject();
    json.addProperty("instances", instances);
    HttpResponse response = doPut("/v2/apps/" + appId + "/flows/" + flowId + "/flowlets/" +
                                    flowletId + "/instances", json.toString());
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }


  @Test
  public void testGetSetFlowletInstances() throws Exception {
    //deploy, check the status and start a flow. Also check the status
    deploy(WordCountApp.class);
    Assert.assertEquals("STOPPED", getRunnableStatus("flows", "WordCountApp", "WordCountFlow"));
    Assert.assertEquals(200, getRunnableStartStop("flows", "WordCountApp", "WordCountFlow", "start"));
    waitState("flows", "WordCountApp", "WordCountFlow", "RUNNING");

    // Get instances for a non-existent flowlet, flow, and app.
    HttpResponse response = doGet("/v2/apps/WordCountApp/flows/WordCountFlow/flowlets/XXXX/instances");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    response = doGet("/v2/apps/WordCountApp/flows/XXXX/flowlets/StreamSource/instances");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    response = doGet("/v2/apps/XXXX/flows/WordCountFlow/flowlets/StreamSource/instances");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());


    // PUT instances for non-existent flowlet, flow, and app.
    String payload = "{instances: 1}";
    response = doPut("/v2/apps/WordCountApp/flows/WordCountFlow/flowlets/XXXX/instances", payload);
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    response = doPut("/v2/apps/WordCountApp/flows/XXXX/flowlets/StreamSource/instances", payload);
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    response = doPut("/v2/apps/XXXX/flows/WordCountFlow/flowlets/StreamSource/instances", payload);
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());


    //Get Flowlet Instances
    Assert.assertEquals(1, getFlowletInstances("WordCountApp", "WordCountFlow", "StreamSource"));

    //Set Flowlet Instances
    setFlowletInstances("WordCountApp", "WordCountFlow", "StreamSource", 3);
    Assert.assertEquals(3, getFlowletInstances("WordCountApp", "WordCountFlow", "StreamSource"));

    // Stop the flow and check its status
    Assert.assertEquals(200, getRunnableStartStop("flows", "WordCountApp", "WordCountFlow", "stop"));
    waitState("flows", "WordCountApp", "WordCountFlow", "STOPPED");
  }

  @Test
  public void testChangeFlowletStreamInput() throws Exception {
    deploy(MultiStreamApp.class);

    Assert.assertEquals(200,
                        changeFlowletStreamInput("MultiStreamApp", "CounterFlow", "counter1", "stream1", "stream2"));
    // stream1 is no longer a connection
    Assert.assertEquals(500,
                        changeFlowletStreamInput("MultiStreamApp", "CounterFlow", "counter1", "stream1", "stream3"));
    Assert.assertEquals(200,
                        changeFlowletStreamInput("MultiStreamApp", "CounterFlow", "counter1", "stream2", "stream3"));

    Assert.assertEquals(200,
                        changeFlowletStreamInput("MultiStreamApp", "CounterFlow", "counter2", "stream3", "stream4"));
    // stream1 is no longer a connection
    Assert.assertEquals(500,
                        changeFlowletStreamInput("MultiStreamApp", "CounterFlow", "counter2", "stream3", "stream1"));
    Assert.assertEquals(200,
                        changeFlowletStreamInput("MultiStreamApp", "CounterFlow", "counter2", "stream4", "stream1"));
  }

  private int changeFlowletStreamInput(String app, String flow, String flowlet,
                                       String oldStream, String newStream) throws Exception {
    return doPut(
      String.format("/v2/apps/%s/flows/%s/flowlets/%s/connections/%s", app, flow, flowlet, newStream),
      String.format("{\"oldStreamId\":\"%s\"}", oldStream)).getStatusLine().getStatusCode();
  }*/
}
