package com.continuuity.internal.app.services.http.handlers;

import com.continuuity.DummyAppWithTrackingTable;
import com.continuuity.SleepingWorkflowApp;
import com.continuuity.WordCountApp;
import com.continuuity.api.Application;
import com.continuuity.app.services.ProgramId;
import com.continuuity.internal.app.services.http.AppFabricTestsSuite;
import com.continuuity.test.internal.DefaultId;
import com.continuuity.test.internal.TestHelper;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import com.continuuity.app.services.EntityType;
import org.junit.Test;
import java.util.Map;


/**
 *
 */
public class AppFabricHttpHandlerTest {

  //temporary code , till we move to the http handler deploy.
  public static void deploy(Class<? extends Application> application) throws Exception{
    TestHelper.deployApplication(application);
  }

  private String getRunnableStatus(String runnableType, String appId, String runnableId) throws Exception {
    HttpResponse response =
      AppFabricTestsSuite.doGet("/v2/apps/" + appId + "/" + runnableType + "/" + runnableId + "/status");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Map<String, String> o = new Gson().fromJson(s, new TypeToken<Map<String, String>>() { }.getType());
    return o.get("status");
  }

  private int getFlowletInstances(String appId, String flowId, String flowletId) throws Exception {
    HttpResponse response =
      AppFabricTestsSuite.doGet("/v2/apps/" + appId + "/flows/" + flowId + "/flowlets/"+ flowletId + "/instances");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String result = EntityUtils.toString(response.getEntity());
    Map<String, String> reply = new Gson().fromJson(result, new TypeToken<Map<String, String>>() { }.getType());
    return Integer.parseInt(reply.get("instances"));
  }

  private void setFlowletInstances(String appId, String flowId, String flowletId,int instances) throws Exception {
    JsonObject json = new JsonObject();
    json.addProperty("instances", instances);
    HttpResponse response = AppFabricTestsSuite.doPut("/v2/apps/" + appId + "/flows/" + flowId + "/flowlets/"+ flowletId + "/instances",
                                         json.toString());
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  private int getRunnableStartStop(String runnableType, String appId, String runnableId, String action)
      throws Exception {
    HttpResponse response =
        AppFabricTestsSuite.doPost("/v2/apps/" + appId + "/" + runnableType + "/" + runnableId + "/" + action);
    return response.getStatusLine().getStatusCode();
  }

  @Test
  public void testGetSetFlowletInstances() throws Exception {
    //deploy, check the status and start a flow. Also check the status
    deploy(WordCountApp.class);
    Assert.assertEquals("STOPPED", getRunnableStatus("flows", "WordCountApp", "WordCountFlow"));
    Assert.assertEquals(200, getRunnableStartStop("flows", "WordCountApp", "WordCountFlow", "start"));
    Assert.assertEquals("RUNNING", getRunnableStatus("flows", "WordCountApp", "WordCountFlow"));

    //Get Flowlet Instances
    Assert.assertEquals(1,getFlowletInstances("WordCountApp","WordCountFlow","StreamSource"));

    //Set Flowlet Instances
    setFlowletInstances("WordCountApp","WordCountFlow","StreamSource",3);
    Assert.assertEquals(3,getFlowletInstances("WordCountApp","WordCountFlow","StreamSource"));


    // Stop the flow and check its status
    Assert.assertEquals(200, getRunnableStartStop("flows", "WordCountApp", "WordCountFlow", "stop"));
    Assert.assertEquals("STOPPED", getRunnableStatus("flows", "WordCountApp", "WordCountFlow"));
  }


  @Test
  public void testStartStop() throws Exception {

    //deploy, check the status and start a flow. Also check the status
    deploy(WordCountApp.class);
    Assert.assertEquals("STOPPED", getRunnableStatus("flows", "WordCountApp", "WordCountFlow"));
    Assert.assertEquals(200, getRunnableStartStop("flows", "WordCountApp", "WordCountFlow", "start"));
    Assert.assertEquals("RUNNING", getRunnableStatus("flows", "WordCountApp", "WordCountFlow"));

    // Stop the flow and check its status
    Assert.assertEquals(200, getRunnableStartStop("flows", "WordCountApp", "WordCountFlow", "stop"));
    Assert.assertEquals("STOPPED", getRunnableStatus("flows", "WordCountApp", "WordCountFlow"));

    // Check the start/stop endpoints for procedures
    Assert.assertEquals("STOPPED", getRunnableStatus("procedures", "WordCountApp", "WordFrequency"));
    Assert.assertEquals(200, getRunnableStartStop("procedures", "WordCountApp", "WordFrequency", "start"));
    Assert.assertEquals("RUNNING", getRunnableStatus("procedures", "WordCountApp", "WordFrequency"));
    Assert.assertEquals(200, getRunnableStartStop("procedures", "WordCountApp", "WordFrequency", "stop"));
    Assert.assertEquals("STOPPED", getRunnableStatus("procedures", "WordCountApp", "WordFrequency"));

    //start map-reduce and check status and stop the map-reduce job and check the status ..
    deploy(DummyAppWithTrackingTable.class);
    Assert.assertEquals(200, getRunnableStartStop("mapreduce", "dummy", "dummy-batch", "start"));
    Assert.assertEquals("RUNNING", getRunnableStatus("mapreduce", "dummy", "dummy-batch"));
    Assert.assertEquals(200, getRunnableStartStop("mapreduce", "dummy", "dummy-batch", "stop"));
    Assert.assertEquals("STOPPED", getRunnableStatus("mapreduce", "dummy", "dummy-batch"));

    //deploy and check status of a workflow
    deploy(SleepingWorkflowApp.class);
    Assert.assertEquals(200, getRunnableStartStop("workflows", "SleepWorkflowApp", "SleepWorkflow", "start"));
    Assert.assertEquals("RUNNING", getRunnableStatus("workflows", "SleepWorkflowApp", "SleepWorkflow"));
  }

  @Test
  public void testStatus() throws Exception {

    //deploy and check the status
    deploy(WordCountApp.class);
    Assert.assertEquals("STOPPED", getRunnableStatus("flows", "WordCountApp", "WordCountFlow"));

    //start flow and check the status
    ProgramId flowId = new ProgramId(DefaultId.DEFAULT_ACCOUNT_ID, "WordCountApp", "WordCountFlow");
    AppFabricTestsSuite.startProgram(flowId);
    Assert.assertEquals("RUNNING", getRunnableStatus("flows", "WordCountApp", "WordCountFlow"));

    //stop the flow and check the status
    AppFabricTestsSuite.stopProgram(flowId);
    Assert.assertEquals("STOPPED", getRunnableStatus("flows", "WordCountApp", "WordCountFlow"));

    //check the status for procedure
    ProgramId procedureId = new ProgramId(DefaultId.DEFAULT_ACCOUNT_ID, "WordCountApp", "WordFrequency");
    procedureId.setType(EntityType.PROCEDURE);
    AppFabricTestsSuite.startProgram(procedureId);
    Assert.assertEquals("RUNNING", getRunnableStatus("procedures", "WordCountApp", "WordFrequency"));
    AppFabricTestsSuite.stopProgram(procedureId);

    //start map-reduce and check status and stop the map-reduce job and check the status ..
    deploy(DummyAppWithTrackingTable.class);
    ProgramId mapreduceId = new ProgramId(DefaultId.DEFAULT_ACCOUNT_ID, "dummy", "dummy-batch");
    mapreduceId.setType(EntityType.MAPREDUCE);
    AppFabricTestsSuite.startProgram(mapreduceId);
    Assert.assertEquals("RUNNING", getRunnableStatus("mapreduce", "dummy", "dummy-batch"));

    //stop the mapreduce program and check the status
    AppFabricTestsSuite.stopProgram(mapreduceId);
    Assert.assertEquals("STOPPED", getRunnableStatus("mapreduce", "dummy", "dummy-batch"));

    //deploy and check status of a workflow
    deploy(SleepingWorkflowApp.class);
    ProgramId workflowId = new ProgramId(DefaultId.DEFAULT_ACCOUNT_ID, "SleepWorkflowApp", "SleepWorkflow");
    workflowId.setType(EntityType.WORKFLOW);
    AppFabricTestsSuite.startProgram(workflowId);
    Assert.assertEquals("RUNNING", getRunnableStatus("workflows", "SleepWorkflowApp", "SleepWorkflow"));
    AppFabricTestsSuite.stopProgram(workflowId);
  }
}
