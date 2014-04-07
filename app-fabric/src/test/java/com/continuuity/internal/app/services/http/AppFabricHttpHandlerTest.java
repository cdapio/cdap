package com.continuuity.internal.app.services.http;

import com.continuuity.AllProgramsApp;
import com.continuuity.WordCountApp;
import com.continuuity.api.Application;
import com.continuuity.app.services.ProgramId;
import com.continuuity.test.internal.DefaultId;
import com.continuuity.test.internal.TestHelper;
import com.google.gson.Gson;
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
    Map<String, String> o = new Gson().fromJson(s, new TypeToken<Map<String, String>>() {}.getType());
    return o.get("status");
  }

  /**
   * @return Webapp status
   */
  private String getWebappStatus(String appId) throws Exception {
    HttpResponse response =
      AppFabricTestsSuite.doGet("/v2/apps/" + appId + "/" + "webapp" + "/status");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Map<String, String> o = new Gson().fromJson(s, new TypeToken<Map<String, String>>() {}.getType());
    return o.get("status");
  }

  @Test
  public void tesStatus() throws Exception {

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


    //start map-reduce and check status and stop the map-reduce job and check the status ..

    ProgramId mapreduceId = new ProgramId(DefaultId.DEFAULT_ACCOUNT_ID, "WordCountApp", "VoidMapReduceJob");
    mapreduceId.setType(EntityType.MAPREDUCE);
    AppFabricTestsSuite.startProgram(mapreduceId);
    Assert.assertEquals("RUNNING", getRunnableStatus("mapreduce", "WordCountApp", "VoidMapReduceJob"));

    //stop the flow and check the status
    AppFabricTestsSuite.stopProgram(mapreduceId);
    Assert.assertEquals("STOPPED", getRunnableStatus("mapreduce", "WordCountApp", "VoidMapReduceJob"));

    //deploy and check the status
    deploy(AllProgramsApp.class);
    Assert.assertEquals("STOPPED", getRunnableStatus("flows", "App", "NoOpFlow"));

    //check the status for procedure
    ProgramId workflowId = new ProgramId(DefaultId.DEFAULT_ACCOUNT_ID, "App", "NoOpWorkflow");
    workflowId.setType(EntityType.WORKFLOW);
    AppFabricTestsSuite.startProgram(workflowId);
    Assert.assertEquals("RUNNING", getRunnableStatus("workflows", "App", "NoOpWorkflow"));


  }

}
