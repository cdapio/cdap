package com.continuuity.internal.app.services.http.handlers;

import com.continuuity.DummyAppWithTrackingTable;
import com.continuuity.SleepingWorkflowApp;
import com.continuuity.WordCountApp;
import com.continuuity.api.Application;
import com.continuuity.app.services.ProgramId;
import com.continuuity.internal.app.services.http.AppFabricTestsSuite;
import com.continuuity.test.internal.DefaultId;
import com.continuuity.test.internal.TestHelper;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.List;
import java.util.concurrent.TimeUnit;
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

  private static final Gson GSON = new Gson();

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

  private int getRunnableStartStop(String runnableType, String appId, String runnableId, String action)
      throws Exception {
    HttpResponse response =
        AppFabricTestsSuite.doPost("/v2/apps/" + appId + "/" + runnableType + "/" + runnableId + "/" + action);
    return response.getStatusLine().getStatusCode();
  }

  /**
   * Tests history of a flow.
   */
  @Test
  public void testFlowHistory() throws Exception {
    try {
      deploy(WordCountApp.class);
      Assert.assertEquals(200,
          AppFabricTestsSuite.doPost("/v2/apps/WordCountApp/flows/WordCountFlow/start", null)
              .getStatusLine().getStatusCode());
      Assert.assertEquals(200,
          AppFabricTestsSuite.doPost("/v2/apps/WordCountApp/flows/WordCountFlow/stop", null)
              .getStatusLine().getStatusCode());
      Assert.assertEquals(200,
          AppFabricTestsSuite.doPost("/v2/apps/WordCountApp/flows/WordCountFlow/start", null)
              .getStatusLine().getStatusCode());
      Assert.assertEquals(200,
          AppFabricTestsSuite.doPost("/v2/apps/WordCountApp/flows/WordCountFlow/stop", null)
              .getStatusLine().getStatusCode());

      HttpResponse response = AppFabricTestsSuite.doGet("/v2/apps/WordCountApp/flows/WordCountFlow/history");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      String s = EntityUtils.toString(response.getEntity());
      List<Map<String, String>> o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() { }.getType());

      // We started and stopped twice, so we should have 2 entries.
      // At least twice because it may have been done in other tests too.
      Assert.assertTrue(o.size() >= 2);

      // For each one, we have 4 fields.
      for (Map<String, String> m : o) {
        Assert.assertEquals(4, m.size());
      }
    } finally {
      // TODO find a way to replace this. Once all the endpoints are moved, it should be easier
//      Assert.assertEquals(200, AppFabricTestsSuite.doDelete("/v2/apps").getStatusLine().getStatusCode());
    }
  }

  /**
   * Tests history of a procedure.
   */
  @Test
  public void testProcedureHistory() throws Exception {
    try {
      deploy(WordCountApp.class);
      Assert.assertEquals(200,
          AppFabricTestsSuite.doPost("/v2/apps/WordCountApp/procedures/WordFrequency/start", null)
              .getStatusLine().getStatusCode());
      Assert.assertEquals(200,
          AppFabricTestsSuite.doPost("/v2/apps/WordCountApp/procedures/WordFrequency/stop", null)
              .getStatusLine().getStatusCode());
      Assert.assertEquals(200,
          AppFabricTestsSuite.doPost("/v2/apps/WordCountApp/procedures/WordFrequency/start", null)
              .getStatusLine().getStatusCode());
      Assert.assertEquals(200,
          AppFabricTestsSuite.doPost("/v2/apps/WordCountApp/procedures/WordFrequency/stop", null)
              .getStatusLine().getStatusCode());

      HttpResponse response = AppFabricTestsSuite.doGet("/v2/apps/WordCountApp/procedures/WordFrequency/history");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      String s = EntityUtils.toString(response.getEntity());
      List<Map<String, String>> o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() { }.getType());

      // We started and stopped twice, so we should have 2 entries.
      Assert.assertTrue(o.size() >= 2);

      // For each one, we have 4 fields.
      for (Map<String, String> m : o) {
        Assert.assertEquals(4, m.size());
      }
    } finally {
      // TODO find a way to replace this. Once all the endpoints are moved, it should be easier
//      Assert.assertEquals(200, AppFabricTestsSuite.doDelete("/v2/apps").getStatusLine().getStatusCode());
    }
  }

  /**
   * Tests history of a mapreduce.
   */
  @Test
  public void testMapreduceHistory() throws Exception {
    try {
      deploy(DummyAppWithTrackingTable.class);
      Assert.assertEquals(200,
          AppFabricTestsSuite.doPost("/v2/apps/dummy/mapreduce/dummy-batch/start", null)
              .getStatusLine().getStatusCode());
      Assert.assertEquals(200,
          AppFabricTestsSuite.doPost("/v2/apps/dummy/mapreduce/dummy-batch/stop", null)
              .getStatusLine().getStatusCode());
      Assert.assertEquals(200,
          AppFabricTestsSuite.doPost("/v2/apps/dummy/mapreduce/dummy-batch/start", null)
              .getStatusLine().getStatusCode());
      Assert.assertEquals(200,
          AppFabricTestsSuite.doPost("/v2/apps/dummy/mapreduce/dummy-batch/stop", null)
              .getStatusLine().getStatusCode());

      HttpResponse response = AppFabricTestsSuite.doGet("/v2/apps/dummy/mapreduce/dummy-batch/history");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      String s = EntityUtils.toString(response.getEntity());
      List<Map<String, String>> o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() { }.getType());

      // We started and stopped twice, so we should have 2 entries.
      Assert.assertTrue(o.size() >= 2);

      // For each one, we have 4 fields.
      for (Map<String, String> m : o) {
        Assert.assertEquals(4, m.size());
      }
    } finally {
      // TODO find a way to replace this. Once all the endpoints are moved, it should be easier
//      Assert.assertEquals(200, AppFabricTestsSuite.doDelete("/v2/apps").getStatusLine().getStatusCode());
    }
  }

  /**
   * Tests history of a workflow.
   */
  @Test
  public void testWorkflowHistory() throws Exception {
    try {
      deploy(SleepingWorkflowApp.class);
      Assert.assertEquals(200,
          AppFabricTestsSuite.doPost("/v2/apps/SleepWorkflowApp/workflows/SleepWorkflow/start", null)
              .getStatusLine().getStatusCode());
      TimeUnit.SECONDS.sleep(2); //wait till any running jobs completes.
      Assert.assertEquals(200,
          AppFabricTestsSuite.doPost("/v2/apps/SleepWorkflowApp/workflows/SleepWorkflow/start", null)
              .getStatusLine().getStatusCode());
      TimeUnit.SECONDS.sleep(2); //wait till any running jobs completes.

      HttpResponse response = AppFabricTestsSuite.doGet("/v2/apps/SleepWorkflowApp/workflows/SleepWorkflow/history");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      String s = EntityUtils.toString(response.getEntity());
      List<Map<String, String>> o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() { }.getType());

      // We started and stopped twice, so we should have 2 entries.
      Assert.assertTrue(o.size() >= 2);

      // For each one, we have 4 fields.
      for (Map<String, String> m : o) {
        Assert.assertEquals(4, m.size());
      }
    } finally {
      // TODO find a way to replace this. Once all the endpoints are moved, it should be easier
//      Assert.assertEquals(200, AppFabricTestsSuite.doDelete("/v2/apps").getStatusLine().getStatusCode());
    }
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


  @Test
  public void testFlowRuntimeArgs() throws Exception {
    deploy(WordCountApp.class);

    Map<String, String> args = Maps.newHashMap();
    args.put("Key1", "Val1");
    args.put("Key2", "Val1");
    args.put("Key2", "Val1");

    HttpResponse response;
    String argString = GSON.toJson(args, new TypeToken<Map<String, String>>() { }.getType());
    response = AppFabricTestsSuite.doPut("/v2/apps/WordCountApp/flows/WordCountFlow/runtimeargs",
        argString);

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = AppFabricTestsSuite.doGet("/v2/apps/WordCountApp/flows/WordCountFlow/runtimeargs");

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Map<String, String> argsRead = GSON.fromJson(EntityUtils.toString(response.getEntity()),
        new TypeToken<Map<String, String>>() { }.getType());

    Assert.assertEquals(args.size(), argsRead.size());

    for (Map.Entry<String, String> entry : args.entrySet()) {
      Assert.assertEquals(entry.getValue(), argsRead.get(entry.getKey()));
    }

    //test empty runtime args
    response = AppFabricTestsSuite.doPut("/v2/apps/WordCountApp/flows/WordCountFlow/runtimeargs", "");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = AppFabricTestsSuite.doGet("/v2/apps/WordCountApp/flows/WordCountFlow/runtimeargs");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    argsRead = GSON.fromJson(EntityUtils.toString(response.getEntity()),
        new TypeToken<Map<String, String>>() { }.getType());
    Assert.assertEquals(0, argsRead.size());

    //test null runtime args
    response = AppFabricTestsSuite.doPut("/v2/apps/WordCountApp/flows/WordCountFlow/runtimeargs", null);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = AppFabricTestsSuite.doGet("/v2/apps/WordCountApp/flows/WordCountFlow/runtimeargs");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    argsRead = GSON.fromJson(EntityUtils.toString(response.getEntity()),
        new TypeToken<Map<String, String>>() { }.getType());
    Assert.assertEquals(0, argsRead.size());
  }

  @Test
  public void testWorkflowRuntimeArgs() throws Exception {
    deploy(SleepingWorkflowApp.class);
    Map<String, String> args = Maps.newHashMap();
    args.put("Key1", "Val1");
    args.put("Key2", "Val1");
    args.put("Key2", "Val1");

    HttpResponse response;
    String argString = GSON.toJson(args, new TypeToken<Map<String, String>>() { }.getType());
    response = AppFabricTestsSuite.doPut("/v2/apps/SleepWorkflowApp/workflows/SleepWorkflow/runtimeargs",
        argString);

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = AppFabricTestsSuite.doGet("/v2/apps/SleepWorkflowApp/workflows/SleepWorkflow/runtimeargs");

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Map<String, String> argsRead = GSON.fromJson(EntityUtils.toString(response.getEntity()),
        new TypeToken<Map<String, String>>() { }.getType());

    Assert.assertEquals(args.size(), argsRead.size());

    for (Map.Entry<String, String> entry : args.entrySet()) {
      Assert.assertEquals(entry.getValue(), argsRead.get(entry.getKey()));
    }

    //test empty runtime args
    response = AppFabricTestsSuite.doPut("/v2/apps/SleepWorkflowApp/workflows/SleepWorkflow/runtimeargs", "");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = AppFabricTestsSuite.doGet("/v2/apps/SleepWorkflowApp/workflows/SleepWorkflow/runtimeargs");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    argsRead = GSON.fromJson(EntityUtils.toString(response.getEntity()),
        new TypeToken<Map<String, String>>() { }.getType());
    Assert.assertEquals(0, argsRead.size());

    //test null runtime args
    response = AppFabricTestsSuite.doPut("/v2/apps/SleepWorkflowApp/workflows/SleepWorkflow/runtimeargs", null);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = AppFabricTestsSuite.doGet("/v2/apps/SleepWorkflowApp/workflows/SleepWorkflow/runtimeargs");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    argsRead = GSON.fromJson(EntityUtils.toString(response.getEntity()),
        new TypeToken<Map<String, String>>() { }.getType());
    Assert.assertEquals(0, argsRead.size());
  }
}
