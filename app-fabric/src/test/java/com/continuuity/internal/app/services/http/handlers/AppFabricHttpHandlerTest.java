package com.continuuity.internal.app.services.http.handlers;

import com.continuuity.AppWithSchedule;
import com.continuuity.AppWithWorkflow;
import com.continuuity.DummyAppWithTrackingTable;
import com.continuuity.MultiStreamApp;
import com.continuuity.SleepingWorkflowApp;
import com.continuuity.WordCountApp;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.data.dataset.table.Row;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.app.program.ManifestFields;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.DequeueStrategy;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.queue.QueueEntry;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionContext;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.persist.TransactionSnapshot;
import com.continuuity.data2.transaction.snapshot.SnapshotCodec;
import com.continuuity.data2.transaction.snapshot.SnapshotCodecProvider;
import com.continuuity.gateway.handlers.dataset.DataSetInstantiatorFromMetaData;
import com.continuuity.internal.app.services.http.AppFabricTestBase;
import com.continuuity.test.SlowTests;
import com.continuuity.test.XSlowTests;
import com.continuuity.test.internal.DefaultId;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.apache.twill.internal.utils.Dependencies;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import javax.annotation.Nullable;

import static com.continuuity.common.conf.Constants.DEVELOPER_ACCOUNT_ID;


/**
 * Test {@link com.continuuity.gateway.handlers.AppFabricHttpHandler}
 */
public class AppFabricHttpHandlerTest extends AppFabricTestBase {

  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type LIST_MAP_STRING_STRING_TYPE = new TypeToken<List<Map<String, String>>>() { }.getType();
  private static final OperationContext DEFAULT_CONTEXT = new OperationContext(DEVELOPER_ACCOUNT_ID);

  private String getRunnableStatus(String runnableType, String appId, String runnableId) throws Exception {
    HttpResponse response =
      doGet("/v2/apps/" + appId + "/" + runnableType + "/" + runnableId + "/status");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Map<String, String> o = GSON.fromJson(s, new TypeToken<Map<String, String>>() { }.getType());
    return o.get("status");
  }

  private int getFlowletInstances(String appId, String flowId, String flowletId) throws Exception {
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
  private String getDeploymentStatus() throws Exception {
    HttpResponse response =
      doGet("/v2/deploy/status/");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Map<String, String> o = new Gson().fromJson(s, new TypeToken<Map<String, String>>() { }.getType());
    return o.get("status");
  }

  private int getRunnableStartStop(String runnableType, String appId, String runnableId, String action)
    throws Exception {
    HttpResponse response =
      doPost("/v2/apps/" + appId + "/" + runnableType + "/" + runnableId + "/" + action);
    return response.getStatusLine().getStatusCode();
  }

  private void testHistory(Class<?> app, String appId, String runnableType, String runnableId,
                           boolean waitStop, int duration)
      throws Exception {
    try {
      deploy(app);
      Assert.assertEquals(200,
          doPost("/v2/apps/" + appId + "/" + runnableType + "/" + runnableId + "/start", null)
              .getStatusLine().getStatusCode()
      );
      if (waitStop) {
        TimeUnit.SECONDS.sleep(duration);
      } else {
        Assert.assertEquals(200,
            doPost("/v2/apps/" + appId + "/" + runnableType + "/" + runnableId + "/stop", null)
                .getStatusLine().getStatusCode()
        );
      }
      // Sleep to let stop states settle down (for MapReduce).
      TimeUnit.SECONDS.sleep(5);
      Assert.assertEquals(200,
          doPost("/v2/apps/" + appId + "/" + runnableType + "/" + runnableId + "/start", null)
              .getStatusLine().getStatusCode()
      );
      if (waitStop) {
        TimeUnit.SECONDS.sleep(duration);
      } else {
        Assert.assertEquals(200,
            doPost("/v2/apps/" + appId + "/" + runnableType + "/" + runnableId + "/stop", null)
                .getStatusLine().getStatusCode()
        );
      }

      String url = String.format("/v2/apps/%s/%s/%s/history", appId, runnableType, runnableId);
      historyStatusWithRetry(url, 2);

      } finally {
      Assert.assertEquals(200, doDelete("/v2/apps/" + appId).getStatusLine().getStatusCode());
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
          Assert.assertEquals(4, m.size());
        }
        break;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertTrue(trials < 5);
  }

  private void testRuntimeArgs(Class<?> app, String appId, String runnableType, String runnableId)
      throws Exception {
    deploy(app);

    Map<String, String> args = Maps.newHashMap();
    args.put("Key1", "Val1");
    args.put("Key2", "Val1");
    args.put("Key2", "Val1");

    HttpResponse response;
    String argString = GSON.toJson(args, new TypeToken<Map<String, String>>() { }.getType());
    response = doPut("/v2/apps/" + appId + "/" + runnableType + "/" +
                                            runnableId + "/runtimeargs", argString);

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doGet("/v2/apps/" + appId + "/" + runnableType + "/" + runnableId + "/runtimeargs");

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Map<String, String> argsRead = GSON.fromJson(EntityUtils.toString(response.getEntity()),
        new TypeToken<Map<String, String>>() { }.getType());

    Assert.assertEquals(args.size(), argsRead.size());

    for (Map.Entry<String, String> entry : args.entrySet()) {
      Assert.assertEquals(entry.getValue(), argsRead.get(entry.getKey()));
    }

    //test empty runtime args
    response = doPut("/v2/apps/" + appId + "/" + runnableType + "/"
                                            + runnableId + "/runtimeargs", "");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = doGet("/v2/apps/" + appId + "/" + runnableType + "/" + runnableId + "/runtimeargs");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    argsRead = GSON.fromJson(EntityUtils.toString(response.getEntity()),
        new TypeToken<Map<String, String>>() { }.getType());
    Assert.assertEquals(0, argsRead.size());

    //test null runtime args
    response = doPut("/v2/apps/" + appId + "/" + runnableType + "/"
                                            + runnableId + "/runtimeargs", null);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = doGet("/v2/apps/" + appId + "/" + runnableType + "/"
                                            + runnableId + "/runtimeargs");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    argsRead = GSON.fromJson(EntityUtils.toString(response.getEntity()),
        new TypeToken<Map<String, String>>() { }.getType());
    Assert.assertEquals(0, argsRead.size());
  }

  /**
   * Ping test
   */
  @Test
  public void pingTest() throws Exception {
    HttpResponse response = doGet("/v2/ping");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  /**
   * Tests history of a flow.
   */
  @Category(SlowTests.class)
  @Test
  public void testFlowHistory() throws Exception {
    testHistory(WordCountApp.class, "WordCountApp", "flows", "WordCountFlow", false, 0);
  }

  /**
   * Tests history of a procedure.
   */
  @Test
  public void testProcedureHistory() throws Exception {
    testHistory(WordCountApp.class, "WordCountApp", "procedures", "WordFrequency", false, 0);
  }

  /**
   * Tests history of a mapreduce.
   */
  @Category(XSlowTests.class)
  @Test
  public void testMapreduceHistory() throws Exception {
    testHistory(DummyAppWithTrackingTable.class, "dummy", "mapreduce", "dummy-batch", false, 0);
  }

  /**
   * Tests history of a workflow.
   */
  @Category(XSlowTests.class)
  @Test
  public void testWorkflowHistory() throws Exception {
    testHistory(SleepingWorkflowApp.class, "SleepWorkflowApp", "workflows", "SleepWorkflow", true, 2);
  }

  @Test
  public void testGetSetFlowletInstances() throws Exception {
    //deploy, check the status and start a flow. Also check the status
    deploy(WordCountApp.class);
    Assert.assertEquals("STOPPED", getRunnableStatus("flows", "WordCountApp", "WordCountFlow"));
    Assert.assertEquals(200, getRunnableStartStop("flows", "WordCountApp", "WordCountFlow", "start"));
    Assert.assertEquals("RUNNING", getRunnableStatus("flows", "WordCountApp", "WordCountFlow"));

    //Get Flowlet Instances
    Assert.assertEquals(1, getFlowletInstances("WordCountApp", "WordCountFlow", "StreamSource"));

    //Set Flowlet Instances
    setFlowletInstances("WordCountApp", "WordCountFlow", "StreamSource", 3);
    Assert.assertEquals(3, getFlowletInstances("WordCountApp", "WordCountFlow", "StreamSource"));

    // Stop the flow and check its status
    Assert.assertEquals(200, getRunnableStartStop("flows", "WordCountApp", "WordCountFlow", "stop"));
    Assert.assertEquals("STOPPED", getRunnableStatus("flows", "WordCountApp", "WordCountFlow"));
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
  }


  @Category(XSlowTests.class)
  @Test
  public void testStartStop() throws Exception {
    //deploy, check the status and start a flow. Also check the status
    deploy(WordCountApp.class);
    Assert.assertEquals("STOPPED", getRunnableStatus("flows", "WordCountApp", "WordCountFlow"));
    Assert.assertEquals(200, getRunnableStartStop("flows", "WordCountApp", "WordCountFlow", "start"));
    Assert.assertEquals("RUNNING", getRunnableStatus("flows", "WordCountApp", "WordCountFlow"));

    //web-app, start, stop and status check.
    Assert.assertEquals(200,
      doPost("/v2/apps/WordCountApp/webapp/start", null).getStatusLine().getStatusCode());

    Assert.assertEquals("RUNNING", getWebappStatus("WordCountApp"));
    Assert.assertEquals(200,
                        doPost("/v2/apps/WordCountApp/webapp/stop", null)
                          .getStatusLine().getStatusCode());
    Assert.assertEquals("STOPPED", getWebappStatus("WordCountApp"));

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
    while ("STARTING".equals(getRunnableStatus("workflows", "SleepWorkflowApp", "SleepWorkflow"))) {
      TimeUnit.MILLISECONDS.sleep(10);
    }
    Assert.assertEquals("RUNNING", getRunnableStatus("workflows", "SleepWorkflowApp", "SleepWorkflow"));
  }

  /**
   * Metadata tests through appfabric apis.
   */
  @Test
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
      Assert.assertNotNull(map.get("specification"));
      DataSetSpecification spec = new Gson().fromJson(map.get("specification"), DataSetSpecification.class);
      Assert.assertNotNull(spec);

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
  }

  /**
   * Tests procedure instances.
   */
  @Test
  public void testProcedureInstances () throws Exception {
    Assert.assertEquals(200, doDelete("/v2/apps").getStatusLine().getStatusCode());
    Assert.assertEquals(200, doPost("/v2/unrecoverable/reset").getStatusLine().getStatusCode());

    HttpResponse response = deploy(WordCountApp.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = doGet("/v2/apps/WordCountApp/procedures/WordFrequency/instances");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    String s = EntityUtils.toString(response.getEntity());
    Map<String, String> result = new Gson().fromJson(s, MAP_STRING_STRING_TYPE);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(1, Integer.parseInt(result.get("instances")));

    JsonObject json = new JsonObject();
    json.addProperty("instances", 10);

    response = doPut("/v2/apps/WordCountApp/procedures/WordFrequency/instances", json.toString());
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = doGet("/v2/apps/WordCountApp/procedures/WordFrequency/instances");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    s = EntityUtils.toString(response.getEntity());
    result = new Gson().fromJson(s, MAP_STRING_STRING_TYPE);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(10, Integer.parseInt(result.get("instances")));

    Assert.assertEquals(200, doDelete("/v2/apps/WordCountApp").getStatusLine().getStatusCode());
  }

  @Category(XSlowTests.class)
  @Test
  public void testStatus() throws Exception {

    //deploy and check the status
    deploy(WordCountApp.class);
    //check the status of the deployment
    Assert.assertEquals("DEPLOYED", getDeploymentStatus());
    Assert.assertEquals("STOPPED", getRunnableStatus("flows", "WordCountApp", "WordCountFlow"));

    //start flow and check the status
    Assert.assertEquals(200, getRunnableStartStop("flows", "WordCountApp", "WordCountFlow", "start"));
    Assert.assertEquals("RUNNING", getRunnableStatus("flows", "WordCountApp", "WordCountFlow"));

    //stop the flow and check the status
    Assert.assertEquals(200, getRunnableStartStop("flows", "WordCountApp", "WordCountFlow", "stop"));
    Assert.assertEquals("STOPPED", getRunnableStatus("flows", "WordCountApp", "WordCountFlow"));

    //check the status for procedure
    Assert.assertEquals(200, getRunnableStartStop("procedures", "WordCountApp", "WordFrequency", "start"));
    Assert.assertEquals("RUNNING", getRunnableStatus("procedures", "WordCountApp", "WordFrequency"));
    Assert.assertEquals(200, getRunnableStartStop("procedures", "WordCountApp", "WordFrequency", "stop"));

    deploy(DummyAppWithTrackingTable.class);
    //start map-reduce and check status and stop the map-reduce job and check the status ..
    Assert.assertEquals(200, getRunnableStartStop("mapreduce", "dummy", "dummy-batch", "start"));
    Assert.assertEquals("RUNNING", getRunnableStatus("mapreduce", "dummy", "dummy-batch"));

    //stop the mapreduce program and check the status
    Assert.assertEquals(200, getRunnableStartStop("mapreduce", "dummy", "dummy-batch", "stop"));
    Assert.assertEquals("STOPPED", getRunnableStatus("mapreduce", "dummy", "dummy-batch"));

    //deploy and check status of a workflow
    deploy(SleepingWorkflowApp.class);
    Assert.assertEquals(200, getRunnableStartStop("workflows", "SleepWorkflowApp", "SleepWorkflow", "start"));
    while ("STARTING".equals(getRunnableStatus("workflows", "SleepWorkflowApp", "SleepWorkflow"))) {
      TimeUnit.MILLISECONDS.sleep(10);
    }
    Assert.assertEquals("RUNNING", getRunnableStatus("workflows", "SleepWorkflowApp", "SleepWorkflow"));
  }

  private String getWebappStatus(String appId) throws Exception {
    HttpResponse response = doGet("/v2/apps/" + appId + "/" + "webapp" + "/status");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Map<String, String> o = new Gson().fromJson(s, MAP_STRING_STRING_TYPE);
    return o.get("status");
  }

  @Test
  public void testFlowRuntimeArgs() throws Exception {
    testRuntimeArgs(WordCountApp.class, "WordCountApp", "flows", "WordCountFlow");
  }

  @Test
  public void testWorkflowRuntimeArgs() throws Exception {
    testRuntimeArgs(SleepingWorkflowApp.class, "SleepWorkflowApp", "workflows", "SleepWorkflow");
  }

  @Test
  public void testProcedureRuntimeArgs() throws Exception {
    testRuntimeArgs(WordCountApp.class, "WordCountApp", "procedures", "WordFrequency");
  }

  @Test
  public void testMapreduceRuntimeArgs() throws Exception {
    testRuntimeArgs(DummyAppWithTrackingTable.class, "dummy", "mapreduce", "dummy-batch");
  }

  /**
   * Deploys and application.
   */
  public static HttpResponse deploy(Class<?> application) throws Exception {
    return deploy(application, null);
  }
  /**
   * Deploys and application with (optionally) defined app name
   */
  public static HttpResponse deploy(Class<?> application, @Nullable String appName) throws Exception {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(ManifestFields.MAIN_CLASS, application.getName());

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    final JarOutputStream jarOut = new JarOutputStream(bos, manifest);
    final String pkgName = application.getPackage().getName();

    // Grab every classes under the application class package.
    try {
      ClassLoader classLoader = application.getClassLoader();
      if (classLoader == null) {
        classLoader = ClassLoader.getSystemClassLoader();
      }
      Dependencies.findClassDependencies(classLoader, new Dependencies.ClassAcceptor() {
        @Override
        public boolean accept(String className, URL classUrl, URL classPathUrl) {
          try {
            if (className.startsWith(pkgName)) {
              jarOut.putNextEntry(new JarEntry(className.replace('.', '/') + ".class"));
              InputStream in = classUrl.openStream();
              try {
                ByteStreams.copy(in, jarOut);
              } finally {
                in.close();
              }
              return true;
            }
            return false;
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      }, application.getName());

      // Add webapp
      jarOut.putNextEntry(new ZipEntry("webapp/default/netlens/src/1.txt"));
      ByteStreams.copy(new ByteArrayInputStream("dummy data".getBytes(Charsets.UTF_8)), jarOut);
    } finally {
      jarOut.close();
    }

    HttpEntityEnclosingRequestBase request;
    if (appName == null) {
      request = getPost("/v2/apps");
    } else {
      request = getPut("/v2/apps/" + appName);
    }
    request.setHeader(Constants.Gateway.CONTINUUITY_API_KEY, "api-key-example");
    request.setHeader("X-Archive-Name", application.getSimpleName() + ".jar");
    request.setEntity(new ByteArrayEntity(bos.toByteArray()));
    return execute(request);
  }

  /**
   * Tests deploying an application.
   */
  @Test
  public void testDeploy() throws Exception {
    HttpResponse response = deploy(WordCountApp.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  /**
   * Tests taking a snapshot of the transaction manager.
   */
  @Test
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
  }

  /**
   * Tests invalidating a transaction.
   * @throws Exception
   */
  @Test
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
  }

  @Test
  public void testResetTxManagerState() throws Exception {
    HttpResponse response = doPost("/v2/transactions/state");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  /**
   * Tests deploying an application.
   */
  @Test
  public void testDeployInvalid() throws Exception {
    HttpResponse response = deploy(String.class);
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());
    Assert.assertNotNull(response.getEntity());
    Assert.assertTrue(response.getEntity().getContentLength() > 0);
  }

  /**
   * Tests deleting an application.
   */
  @Test
  public void testDelete() throws Exception {
    //Delete an invalid app
    HttpResponse response = doDelete("/v2/apps/XYZ");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
    deploy(WordCountApp.class);
    getRunnableStartStop("flows", "WordCountApp", "WordCountFlow", "start");
    //Try to delete an App while its flow is running
    response = doDelete("/v2/apps/WordCountApp");
    Assert.assertEquals(403, response.getStatusLine().getStatusCode());
    getRunnableStartStop("flows", "WordCountApp", "WordCountFlow", "stop");
    //Delete the App after stopping the flow
    response = doDelete("/v2/apps/WordCountApp");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doDelete("/v2/apps/WordCountApp");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
  }

  /**
   * Tests for program list calls
   */
  @Test
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
  }

  /**
   * Test for schedule handlers.
   */
  @Category(XSlowTests.class)
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

    TimeUnit.SECONDS.sleep(5);
    response = doGet("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/history");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    json = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> history = new Gson().fromJson(json, LIST_MAP_STRING_STRING_TYPE);

    int workflowRuns = history.size();
    Assert.assertTrue(workflowRuns >= 1);

    //Check suspend status
    String scheduleStatus = String.format("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/schedules/%s/status",
                                          scheduleId);
    response = doGet(scheduleStatus);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    json = EntityUtils.toString(response.getEntity());
    Map<String, String> output = new Gson().fromJson(json, MAP_STRING_STRING_TYPE);
    Assert.assertEquals("SCHEDULED", output.get("status"));

    String scheduleSuspend = String.format("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/schedules/%s/suspend",
                                           scheduleId);

    response = doPost(scheduleSuspend);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    //check paused state
    scheduleStatus = String.format("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/schedules/%s/status", scheduleId);
    response = doGet(scheduleStatus);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    json = EntityUtils.toString(response.getEntity());
    output = new Gson().fromJson(json, MAP_STRING_STRING_TYPE);
    Assert.assertEquals("SUSPENDED", output.get("status"));

    TimeUnit.SECONDS.sleep(2); //wait till any running jobs just before suspend call completes.

    response = doGet("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/history");
    json = EntityUtils.toString(response.getEntity());
    history = new Gson().fromJson(json,
                                  LIST_MAP_STRING_STRING_TYPE);
    workflowRuns = history.size();

    //Sleep for some time and verify there are no more scheduled jobs after the suspend.
    TimeUnit.SECONDS.sleep(10);

    response = doGet("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/history");
    json = EntityUtils.toString(response.getEntity());
    history = new Gson().fromJson(json,
                                  LIST_MAP_STRING_STRING_TYPE);
    int workflowRunsAfterSuspend = history.size();
    Assert.assertEquals(workflowRuns, workflowRunsAfterSuspend);

    String scheduleResume = String.format("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/schedules/%s/resume",
                                          scheduleId);

    response = doPost(scheduleResume);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    //Sleep for some time and verify there are no more scheduled jobs after the pause.
    TimeUnit.SECONDS.sleep(3);
    response = doGet("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/history");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    json = EntityUtils.toString(response.getEntity());
    history = new Gson().fromJson(json,
                                  LIST_MAP_STRING_STRING_TYPE);

    int workflowRunsAfterResume = history.size();
    //Verify there is atleast one run after the pause
    Assert.assertTrue(workflowRunsAfterResume > workflowRunsAfterSuspend + 1);

    //check scheduled state
    scheduleStatus = String.format("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/schedules/%s/status", scheduleId);
    response = doGet(scheduleStatus);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    json = EntityUtils.toString(response.getEntity());
    output = new Gson().fromJson(json, MAP_STRING_STRING_TYPE);
    Assert.assertEquals("SCHEDULED", output.get("status"));

    //Check status of a non existing schedule
    String notFoundSchedule = String.format("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/schedules/%s/status",
                                            "invalidId");

    response = doGet(notFoundSchedule);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    json = EntityUtils.toString(response.getEntity());
    output = new Gson().fromJson(json, MAP_STRING_STRING_TYPE);
    Assert.assertEquals("NOT_FOUND", output.get("status"));

    response = doPost(scheduleSuspend);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    //check paused state
    response = doGet(scheduleStatus);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    json = EntityUtils.toString(response.getEntity());
    output = new Gson().fromJson(json, MAP_STRING_STRING_TYPE);
    Assert.assertEquals("SUSPENDED", output.get("status"));

    TimeUnit.SECONDS.sleep(2); //wait till any running jobs just before suspend call completes.

    response = doDelete("/v2/apps/AppWithSchedule");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testTableReads() throws Exception {
    DataSetInstantiatorFromMetaData instantiator =
      AppFabricTestBase.getInjector().getInstance(DataSetInstantiatorFromMetaData.class);

    Table t = newTable("tTR_" + System.nanoTime(), instantiator);
    // write a row with 10 cols c0...c9 with values v0..v9
    String row = "tTR10";
    byte[] rowKey = row.getBytes();
    byte[][] cols = new byte[10][];
    byte[][] vals = new byte[10][];
    for (int i = 0; i < 10; ++i) {
      cols[i] = ("c" + i).getBytes();
      vals[i] = ("v" + i).getBytes();
    }

    TransactionSystemClient txClient =
      AppFabricTestBase.getInjector().getInstance(TransactionSystemClient.class);
    TransactionContext txContext =
      new TransactionContext(txClient, instantiator.getInstantiator().getTransactionAware());

    txContext.start();
    t.put(rowKey, cols, vals);
    txContext.finish();

    // now read back in various ways
    String queryPrefix = "/v2/tables/" + t.getName() + "/rows/" + row;
    assertRead(queryPrefix, 0, 9, ""); // all columns
    assertRead(queryPrefix, 5, 5, "?columns=c5"); // only c5
    assertRead(queryPrefix, 3, 5, "?columns=c5,c3,c4"); // only c3,c4, and c5
    assertRead(queryPrefix, 8, 9, "?columns=c8,c9,c10"); // only c8 and c9
    assertRead(queryPrefix, 0, 4, "?stop=c5"); // range up to exclusive 5
    assertRead(queryPrefix, 0, 2, "?stop=c5&limit=3"); // range up to exclusive 5, limit 3 -> c0..c2
    assertRead(queryPrefix, 0, 2, "?stop=c3&limit=5"); // range up to exclusive 3, limit 5 -> c0..c2
    assertRead(queryPrefix, 5, 9, "?start=c5"); // range starting at 5
    assertRead(queryPrefix, 5, 7, "?start=c5&limit=3"); // range starting at 5, limit 3 -> c5..c7
    assertRead(queryPrefix, 0, 4, "?limit=5"); // all limit 5 -> c0..c5
    assertRead(queryPrefix, 0, 9, "?limit=12"); // all limit 12 -> c0..c9
    assertRead(queryPrefix, 2, 5, "?start=c2&stop=c6"); // range from 2 to exclusive 6 -> 2,3,4,5
    assertRead(queryPrefix, 2, 4, "?start=c2&stop=c6&limit=3"); // range from 2 to exclusive 6 limited to 3 -> 2,3,4

    // now read some stuff that returns errors
    assertReadFails(queryPrefix, "/c1", HttpStatus.SC_NOT_FOUND); // path does not end with row
    assertReadFails(queryPrefix, "?columns=c1,c2&start=c5", HttpStatus.SC_BAD_REQUEST); // range and list of columns
    assertReadFails(queryPrefix, "?columns=c10&encoding=hex", HttpStatus.SC_BAD_REQUEST); // col invalid under encoding
    assertReadFails(queryPrefix, "?columns=c10&encoding=blah", HttpStatus.SC_BAD_REQUEST); // bad encoding
    assertReadFails(queryPrefix, "?columns=a", HttpStatus.SC_NO_CONTENT); // non-existing column
    assertReadFails("", "/v2/tables/" + t.getName() + "/rows/abc", HttpStatus.SC_NO_CONTENT); // non-existing row
    assertReadFails("", "/v2/tables/abc/rows/tTR10", HttpStatus.SC_NOT_FOUND); // non-existing table
  }

  @Test
  public void testTableWritesAndDeletes() throws Exception {
    DataSetInstantiatorFromMetaData instantiator =
      AppFabricTestBase.getInjector().getInstance(DataSetInstantiatorFromMetaData.class);
    String urlPrefix = "/v2";
    Table t = newTable("tTW_" + System.nanoTime(), instantiator);
    String row = "abc";
    byte[] c1 = { 'c', '1' }, c2 = { 'c', '2' }, c3 = { 'c', '3' };
    byte[] v1 = { 'v', '1' }, mt = { }, v3 = { 'v', '3' };

    // write a row with 3 cols c1...c3 with values v1, "", v3
    String json = "{\"c1\":\"v1\",\"c2\":\"\",\"c3\":\"v3\"}";
    assertWrite(urlPrefix, HttpStatus.SC_OK, "/tables/" + t.getName() + "/rows/" + row, json);

    // starting new tx so that we see what was committed
    TransactionSystemClient txClient = AppFabricTestBase.getInjector().getInstance(TransactionSystemClient.class);
    TransactionContext txContext = new TransactionContext(txClient,
                                                          instantiator.getInstantiator().getTransactionAware());

    // read back directly and verify
    txContext.start();
    Row result = t.get(row.getBytes());
    Assert.assertFalse(result.isEmpty());
    Assert.assertArrayEquals(v1, result.get(c1));
    byte[] r2 = result.get(c2);
    Assert.assertTrue(null == r2 || Arrays.equals(mt, r2));
    Assert.assertArrayEquals(v3, result.get(c3));

    // delete c1 and c2
    assertDelete(urlPrefix, HttpStatus.SC_OK, "/tables/" + t.getName() + "/rows/" + row + "?columns=c1;columns=c2");

    // starting new tx so that we see what was committed
    txContext.finish();

    txContext.start();

    // read back directly and verify they're gone
    result = t.get(row.getBytes());
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(1, result.getColumns().size());
    Assert.assertNull(result.get(c1));
    Assert.assertNull(result.get(c2));
    Assert.assertArrayEquals(v3, result.get(c3));

    // test some error cases
    assertWrite(urlPrefix, HttpStatus.SC_NOT_FOUND, "/tables/abc/rows/" + row, json); // non-existent table
    assertWrite(urlPrefix, HttpStatus.SC_NOT_FOUND, "/tables/abc/rows/a/x" + row, json); // path does not end with row
    assertWrite(urlPrefix, HttpStatus.SC_BAD_REQUEST, "/tables/" + t.getName() + "/rows/" + row, ""); // no json
    assertWrite(urlPrefix, HttpStatus.SC_BAD_REQUEST, "/tables/" + t.getName() + "/rows/" + row,
                "{\"\"}"); // wrong json

    // we can delete whole row by providing no columns
    assertDelete(urlPrefix, HttpStatus.SC_OK, "/tables/" + t.getName() + "/rows/" + row);
    // specified
    assertDelete(urlPrefix, HttpStatus.SC_NOT_FOUND, "/tables/abc/rows/" + row + "?columns=a"); // non-existent table
    assertDelete(urlPrefix, HttpStatus.SC_METHOD_NOT_ALLOWED, "/tables/" + t.getName()); // no/empty row key
    assertDelete(urlPrefix, HttpStatus.SC_METHOD_NOT_ALLOWED, "/tables//" + t.getName()); // no/empty row key
  }



  @Test
  public void testIncrement() throws Exception {
    DataSetInstantiatorFromMetaData instantiator =
      AppFabricTestBase.getInjector().getInstance(DataSetInstantiatorFromMetaData.class);
    String urlPrefix = "/v2";
    Table t = newTable("tI_" + System.nanoTime(), instantiator);
    String row = "abc";
    // directly write a row with two columns, a long, b not
    final byte[] a = { 'a' }, b = { 'b' }, c = { 'c' };
    TransactionSystemClient txClient =
      AppFabricTestBase.getInjector().getInstance(TransactionSystemClient.class);
    TransactionContext txContext =
      new TransactionContext(txClient, instantiator.getInstantiator().getTransactionAware());

    txContext.start();
    t.put(row.getBytes(), new byte[][] { a, b }, new byte[][] { Bytes.toBytes(7L), b });
    txContext.finish();

    // submit increment for row with c1 and c3, should succeed
    String json = "{\"a\":35, \"c\":11}";
    Map<String, Long> map = assertIncrement(urlPrefix, 200, "/tables/" + t.getName() + "/rows/" + row + "/increment",
                                            json);

    // starting new tx so that we see what was committed
    txContext.start();
    // verify result is the incremented value
    Assert.assertNotNull(map);
    Assert.assertEquals(2L, map.size());
    Assert.assertEquals(new Long(42), map.get("a"));
    Assert.assertEquals(new Long(11), map.get("c"));

    // verify directly incremented has happened
    Row result = t.get(row.getBytes());
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(3, result.getColumns().size());
    Assert.assertArrayEquals(Bytes.toBytes(42L), result.get(a));
    Assert.assertArrayEquals(b, result.get(b));
    Assert.assertArrayEquals(Bytes.toBytes(11L), result.get(c));

    // submit an increment for a and b, must fail with not-a-number
    json = "{\"a\":1,\"b\":12}";
    assertIncrement(urlPrefix, 400, "/tables/" + t.getName() + "/rows/" + row + "/increment", json);

    // starting new tx so that we see what was committed
    txContext.finish();
    txContext.start();
    // verify directly that the row is unchanged
    result = t.get(row.getBytes());
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(3, result.getColumns().size());
    Assert.assertArrayEquals(Bytes.toBytes(42L), result.get(a));
    Assert.assertArrayEquals(b, result.get(b));
    Assert.assertArrayEquals(Bytes.toBytes(11L), result.get(c));

    // submit an increment for non-existent row, should succeed
    json = "{\"a\":1,\"b\":-12}";
    map = assertIncrement(urlPrefix, 200, "/tables/" + t.getName() + "/rows/xyz/increment", json);

    // starting new tx so that we see what was committed
    txContext.finish();
    txContext.start();
    // verify return value is equal to increments
    Assert.assertNotNull(map);
    Assert.assertEquals(2L, map.size());
    Assert.assertEquals(new Long(1), map.get("a"));
    Assert.assertEquals(new Long(-12), map.get("b"));
    // verify directly that new values are there
    // verify directly that the row is unchanged
    result = t.get("xyz".getBytes());
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(2, result.getColumns().size());
    Assert.assertArrayEquals(Bytes.toBytes(1L), result.get(a));
    Assert.assertArrayEquals(Bytes.toBytes(-12L), result.get(b));

    // test some bad cases
    assertIncrement(urlPrefix, 404, "/tables/" + t.getName() + "1/abc", json); // table does not exist
    assertIncrement(urlPrefix, 404, "/tables/" + t.getName() + "1/abc/x", json); // path does not end on row
    assertIncrement(urlPrefix, 400, "/tables/" + t.getName() + "/rows/xyz/increment", "{\"a\":\"b\"}"); // json invalid
    assertIncrement(urlPrefix, 400, "/tables/" + t.getName() + "/rows/xyz/increment", "{\"a\":1"); // json invalid
  }

  @Test
  public void testEncodingOfKeysAndValues() throws Exception {

    DataSetInstantiatorFromMetaData instantiator =
      AppFabricTestBase.getInjector().getInstance(DataSetInstantiatorFromMetaData.class);
    // first create the table
    String tableName = "tEOCAV_" + System.nanoTime();
    Table table = createTable(tableName, instantiator);
    byte[] x = { 'x' }, y = { 'y' }, z = { 'z' }, a = { 'a' };

    // setup accessor
    String urlPrefix = "/v2";
    String tablePrefix = urlPrefix + "/tables/" + tableName + "/rows/";

    // table is empty, write value z to column y of row x, use encoding "url"
    assertWrite(tablePrefix, HttpStatus.SC_OK, "%78" + "?encoding=url", "{\"%79\":\"%7A\"}");
    // read back directly and verify

    // starting new tx so that we see what was committed
    TransactionSystemClient txClient =
      AppFabricTestBase.getInjector().getInstance(TransactionSystemClient.class);
    TransactionContext txContext =
      new TransactionContext(txClient, instantiator.getInstantiator().getTransactionAware());

    txContext.start();
    Row result = table.get(x);
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(1, result.getColumns().size());
    Assert.assertArrayEquals(z, result.get(y));

    // read back with same encoding through REST - the response will not escape y or z
    assertRead(tablePrefix, "%78" + "?columns=%79" + "&encoding=url", "y", "z");
    // read back with hex encoding through REST - the response will escape y and z
    assertRead(tablePrefix, "78" + "?columns=79" + "&encoding=hex", "79", "7a");
    // read back with base64 encoding through REST - the response will escape y and z
    assertRead(tablePrefix, "eA" + "?columns=eQ" + "&encoding=base64", "eQ", "eg");

    // delete using hex encoding
    assertDelete(tablePrefix, 200, "78" + "?columns=79" + "&encoding=hex");

    // starting new tx so that we see what was committed
    txContext.finish();
    txContext.start();
    // and verify that it is really gone
    Row read = table.get(x);
    Assert.assertTrue(read.isEmpty());

    // increment column using REST
    assertIncrement(tablePrefix, 200, "eA" + "/increment" + "?encoding=base64", "{\"YQ\":42}");
    // verify the value was written using the Table
    // starting new tx so that we see what was committed
    txContext.finish();
    txContext.start();
    result = table.get(x);
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(1, result.getColumns().size());
    Assert.assertArrayEquals(Bytes.toBytes(42L), result.get(a));
    // read back via REST with hex encoding
    assertRead(tablePrefix, "eA" + "?column=YQ" + "&encoding=base64", "YQ",
               Base64.encodeBase64URLSafeString(Bytes.toBytes(42L)));
  }


  @Test
  public void testTruncateTable() throws Exception {
    String urlPrefix = "/v2";
    String tablePrefix = urlPrefix + "/tables/";
    String table = "ttTbl_" + System.nanoTime();
    assertCreate(tablePrefix, HttpStatus.SC_OK, table);
    assertWrite(tablePrefix, HttpStatus.SC_OK, table + "/rows/abc", "{ \"c1\":\"v1\"}");
    // make sure both columns are there
    assertRead(tablePrefix, 1, 1, table + "/rows/abc");

    assertTruncate(urlPrefix, HttpStatus.SC_OK, "/datasets/" + table + "/truncate");

    // make sure data was removed: 204 on read
    assertReadFails(tablePrefix, table + "/rows/abc", HttpStatus.SC_NO_CONTENT);

    // but table is there: we can write into it again
    assertCreate(tablePrefix, HttpStatus.SC_OK, table);
    assertWrite(tablePrefix, HttpStatus.SC_OK, table + "/rows/abc", "{ \"c3\":\"v3\"}");
    // make sure both columns are there
    assertRead(tablePrefix, 3, 3, table + "/rows/abc");
  }

  // TODO: Fix this unit test
  @Ignore
  @Test
  public void testClearQueuesStreams() throws Exception {
    // setup accessor
    String tableName = "mannamanna2";
    String streamName = "doobdoobee2";
    String queueName = "doobee2";

    // create a stream, a queue, a table
    DataSetInstantiatorFromMetaData instantiator =
      AppFabricTestBase.getInjector().getInstance(DataSetInstantiatorFromMetaData.class);
    createTable(tableName, instantiator);
    createStream(streamName);
    createQueue(queueName);

    // verify they are all there
    Assert.assertTrue(verifyTable(tableName, instantiator));
    Assert.assertTrue(verifyStream(streamName));
    Assert.assertTrue(verifyQueue(queueName));

    // clear queues
    Assert.assertEquals(200, doDelete("/v2/queues").getStatusLine().getStatusCode());

    // verify tables and streams are still here
    Assert.assertTrue(verifyTable(tableName, instantiator));
    Assert.assertTrue(verifyStream(streamName));
    // verify queue is gone
    Assert.assertFalse(verifyQueue(queueName));

    // recreate the queue
    createQueue(queueName);
    Assert.assertTrue(verifyQueue(queueName));

    // clear streams
    Assert.assertEquals(200, doDelete("/v2/streams").getStatusLine().getStatusCode());

    // verify table and queue are still here
    Assert.assertTrue(verifyTable(tableName, instantiator));
    Assert.assertTrue(verifyQueue(queueName));
    // verify stream is gone
    Assert.assertFalse(verifyStream(streamName));

  }


   static final QueueEntry STREAM_ENTRY = new QueueEntry("x".getBytes());

   void createStream(String name) throws Exception {
    // create stream
    Assert.assertEquals(200, doPut("/v2/streams/" + name).getStatusLine().getStatusCode());

    // write smth to a stream
    QueueName queueName = QueueName.fromStream(name);
    enqueue(queueName, STREAM_ENTRY);
  }

   void createQueue(String name) throws Exception {
    // write smth to a queue
    QueueName queueName = getQueueName(name);
    enqueue(queueName, STREAM_ENTRY);
  }

   boolean dequeueOne(QueueName queueName) throws Exception {
    QueueClientFactory queueClientFactory = AppFabricTestBase.getInjector().getInstance(QueueClientFactory.class);
    final Queue2Consumer consumer = queueClientFactory.createConsumer(queueName,
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

  boolean verifyStream(String name) throws Exception {
    // for now, DELETE /streams only deletes the stream data, not meta data
    // boolean streamExists = 200 ==
    //   doGet("/v2/streams/" + name + "/info").getStatusLine().getStatusCode();
    return dequeueOne(QueueName.fromStream(name));
  }

  boolean verifyQueue(String name) throws Exception {
    return dequeueOne(getQueueName(name));
  }

  boolean verifyTable(String name, DataSetInstantiatorFromMetaData instantiator) throws Exception {
    TransactionSystemClient txClient = AppFabricTestBase.getInjector().getInstance(TransactionSystemClient.class);
    Table table = instantiator.getDataSet(name, DEFAULT_CONTEXT);
    TransactionContext txContext =
      new TransactionContext(txClient, instantiator.getInstantiator().getTransactionAware());
    txContext.start();
    byte[] result = table.get(new byte[]{'a'}, new byte[]{'b'});
    txContext.finish();
    return result != null;
  }

  private  void enqueue(QueueName queueName, final QueueEntry queueEntry) throws Exception {
    QueueClientFactory queueClientFactory = AppFabricTestBase.getInjector().getInstance(QueueClientFactory.class);
    final Queue2Producer producer = queueClientFactory.createProducer(queueName);
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
  }

  private  QueueName getQueueName(String name) {
    // i.e. flow and flowlet are constants: should be good enough
    return QueueName.fromFlowlet("app1", "flow1", "flowlet1", name);
  }

  void assertTruncate(String prefix, int expected, String query) throws Exception {
    HttpResponse response = doPost(prefix + query, "");
    Assert.assertEquals(expected, response.getStatusLine().getStatusCode());
  }

  Table newTable(String name, DataSetInstantiatorFromMetaData instantiator) throws Exception {
    String accountId = DefaultId.DEFAULT_ACCOUNT_ID;
    String spec = new Gson().toJson(new Table(name).configure());
    instantiator.createDataSet(accountId, spec);
    Table table = instantiator.getDataSet(name, DEFAULT_CONTEXT);
    table.getName();
    return table;
  }

   Table createTable(String name, DataSetInstantiatorFromMetaData instantiator) throws Exception {
    TransactionSystemClient txClient =
      AppFabricTestBase.getInjector().getInstance(TransactionSystemClient.class);
    TransactionContext txContext =
      new TransactionContext(txClient, instantiator.getInstantiator().getTransactionAware());
    Table table = newTable(name, instantiator);
    txContext.start();
    table.put(new byte[] {'a'}, new byte[] {'b'}, new byte[] {'c'});
    txContext.finish();
    return table;
  }


   void assertRead(String prefix, int start, int end, String query) throws Exception {
    HttpResponse response = doGet(prefix + query);
    Assert.assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());

    Reader reader = new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8);
    Type stringMapType = MAP_STRING_STRING_TYPE;
    Map<String, String> map = new Gson().fromJson(reader, stringMapType);
    Assert.assertEquals(end - start + 1, map.size());
    for (int i = start; i < end; i++) {
      Assert.assertEquals("v" + i, map.get("c" + i));
    }
  }

  void assertRead(String prefix, String query, String col, String val) throws Exception {
    HttpResponse response = doGet(prefix + query);
    Assert.assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    Reader reader = new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8);
    Type stringMapType = MAP_STRING_STRING_TYPE;
    Map<String, String> map = new Gson().fromJson(reader, stringMapType);
    Assert.assertEquals(1, map.size());
    Assert.assertEquals(val, map.get(col));
  }

  void assertReadFails(String prefix, String query, int expected) throws Exception {
    HttpResponse response = doGet(prefix + query);
    Assert.assertEquals(expected, response.getStatusLine().getStatusCode());
  }
   void assertWrite(String prefix, int expected, String query, String json) throws Exception {
    HttpResponse response = doPut(prefix + query, json);
    Assert.assertEquals(expected, response.getStatusLine().getStatusCode());
  }

   void assertDelete(String prefix, int expected, String query) throws Exception {
    HttpResponse response = doDelete(prefix + query);
    Assert.assertEquals(expected, response.getStatusLine().getStatusCode());
  }

   void assertCreate(String prefix, int expected, String query) throws Exception {
    HttpResponse response = doPut(prefix + query);
    Assert.assertEquals(expected, response.getStatusLine().getStatusCode());
  }

  static Map<String, Long> assertIncrement(String prefix, int expected, String query, String json) throws Exception {
    HttpResponse response = doPost(prefix + query, json);
    Assert.assertEquals(expected, response.getStatusLine().getStatusCode());
    if (expected != HttpStatus.SC_OK) {
      return null;
    }

    Reader reader = new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8);
    // JSon always returns string maps, no matter what the type, must be due to type erasure
    Type valueMapType = MAP_STRING_STRING_TYPE;
    Map<String, String> map = new Gson().fromJson(reader, valueMapType);
    // convert to map(string->long)
    Map<String, Long> longMap = Maps.newHashMap();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      longMap.put(entry.getKey(), Long.parseLong(entry.getValue()));
    }
    return longMap;
  }

  /**
   * Test for resetting app.
   */
  @Test
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
  }


  /**
   * Test for resetting app.
   */
  @Test
  public void testUnRecoverableResetAppRunning() throws Exception {

    HttpResponse response = deploy(WordCountApp.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertEquals(200, getRunnableStartStop("flows", "WordCountApp", "WordCountFlow", "start"));
    Assert.assertEquals("RUNNING", getRunnableStatus("flows", "WordCountApp", "WordCountFlow"));
    response = doPost("/v2/unrecoverable/reset");
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());
    Assert.assertEquals(200, getRunnableStartStop("flows", "WordCountApp", "WordCountFlow", "stop"));
  }

}
