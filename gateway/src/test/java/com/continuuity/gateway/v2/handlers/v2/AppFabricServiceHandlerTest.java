package com.continuuity.gateway.v2.handlers.v2;

import com.continuuity.api.Application;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.app.program.ManifestFields;
import com.continuuity.gateway.GatewayFastTestsSuite;
import com.continuuity.gateway.apps.wordcount.AppWithSchedule;
import com.continuuity.gateway.apps.wordcount.AppWithWorkflow;
import com.continuuity.gateway.apps.wordcount.AssociationTable;
import com.continuuity.gateway.apps.wordcount.UniqueCountTable;
import com.continuuity.gateway.apps.wordcount.WCount;
import com.continuuity.gateway.apps.wordcount.WordCount;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.weave.internal.utils.Dependencies;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;

/**
 * Testing of App Fabric REST Endpoints.
 */
public class AppFabricServiceHandlerTest {

  private static final Gson GSON = new Gson();

  /**
   * Deploys and application.
   */
  private HttpResponse deploy(Class<? extends Application> application) throws Exception {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(ManifestFields.MAIN_CLASS, application.getName());

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    final JarOutputStream jarOut = new JarOutputStream(bos, manifest);
    final String pkgName = application.getPackage().getName();

    // Grab every classes under the application class package.
    try {
      Dependencies.findClassDependencies(application.getClassLoader(), new Dependencies.ClassAcceptor() {
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

    HttpPost post = GatewayFastTestsSuite.getPost("/v2/apps");
    post.setHeader(GatewayAuthenticator.CONTINUUITY_API_KEY, "api-key-example");
    post.setHeader("X-Archive-Name", application.getSimpleName() + ".jar");
    post.setEntity(new ByteArrayEntity(bos.toByteArray()));
    return GatewayFastTestsSuite.doPost(post);
  }

  /**
   * Tests deploying an application.
   */
  @Test
  public void testDeploy() throws Exception {
    HttpResponse response = deploy(WordCount.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  /**
   * Tests deleting an application.
   */
  @Test
  public void testDeleteApp() throws Exception {
    HttpResponse response = deploy(WordCount.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertEquals(200, GatewayFastTestsSuite.doDelete("/v2/apps/WordCount").getStatusLine().getStatusCode());
  }

  /**
   * Test deleting of all applications.
   */
  @Test
  public void testDeleteAllApps() throws Exception {
    HttpResponse response = deploy(WordCount.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertEquals(200, GatewayFastTestsSuite.doDelete("/v2/apps").getStatusLine().getStatusCode());
  }

  /**
   * @return Runnable status
   */
  private String getRunnableStatus(String runnableType, String appId, String runnableId) throws Exception {
    HttpResponse response =
      GatewayFastTestsSuite.doGet("/v2/apps/" + appId + "/" + runnableType + "/" + runnableId + "/status");
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
      GatewayFastTestsSuite.doGet("/v2/apps/" + appId + "/" + "webapp" + "/status");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Map<String, String> o = new Gson().fromJson(s, new TypeToken<Map<String, String>>() {}.getType());
    return o.get("status");
  }

  /**
   * Tests deploying a flow, starting a flow, stopping a flow, test status of non existing flow
   * and deleting the application.
   */
  @Test
  public void testStartStopStatusOfFlow() throws Exception {
    HttpResponse response = deploy(WordCount.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    try {
      Assert.assertEquals(200,
             GatewayFastTestsSuite.doPost("/v2/apps/WordCount/flows/WordCounter/start", null)
               .getStatusLine().getStatusCode()
      );
      Assert.assertEquals("RUNNING", getRunnableStatus("flows", "WordCount", "WordCounter"));
      Assert.assertEquals(403,
                          GatewayFastTestsSuite.doDelete("/v2/apps/WordCount/flows/WordCounter/queues")
                                               .getStatusLine().getStatusCode()
      );
    } finally {
      Assert.assertEquals(200,
                          GatewayFastTestsSuite.doPost("/v2/apps/WordCount/flows/WordCounter/stop", null)
                            .getStatusLine().getStatusCode()
      );
      Assert.assertEquals("STOPPED", getRunnableStatus("flows", "WordCount", "WordCounter"));
      Assert.assertEquals(200,
                          GatewayFastTestsSuite.doDelete("/v2/apps/WordCount/flows/WordCounter/queues")
                                               .getStatusLine().getStatusCode()
      );
    }

    // Test start, stop, status of webapp
    try {
      Assert.assertEquals(200,
                          GatewayFastTestsSuite.doPost("/v2/apps/WordCount/webapp/start", null)
                            .getStatusLine().getStatusCode()
      );
      Assert.assertEquals("RUNNING", getWebappStatus("WordCount"));
    } finally {
      Assert.assertEquals(200,
                          GatewayFastTestsSuite.doPost("/v2/apps/WordCount/webapp/stop", null)
                            .getStatusLine().getStatusCode()
      );
      Assert.assertEquals("STOPPED", getWebappStatus("WordCount"));
    }

    Assert.assertEquals(404, GatewayFastTestsSuite.doGet("/v2/apps/WordCount/flows/" +
                                                           "NonExistingFlow/status")
      .getStatusLine().getStatusCode());

    Assert.assertEquals(404, GatewayFastTestsSuite.doGet("/v2/apps/WordCount/procedures/" +
                                                           "NonExistingProcedure/status")
      .getStatusLine().getStatusCode());

    Assert.assertEquals(404, GatewayFastTestsSuite.doGet("/v2/apps/WordCount/mapreduce/" +
                                                           "NonExistingMapReduce/status")
      .getStatusLine().getStatusCode());

    Assert.assertEquals(404, GatewayFastTestsSuite.doGet("/v2/apps/WordCount/procedures/" +
                                                           "NonExistingProcedure/status")
      .getStatusLine().getStatusCode());

    Assert.assertEquals(200, GatewayFastTestsSuite.doDelete("/v2/apps/WordCount").getStatusLine().getStatusCode());
    Assert.assertEquals(500, GatewayFastTestsSuite.doDelete("/v2/apps/WordCount").getStatusLine().getStatusCode());
  }

  /**
   * Tests procedure instances.
   */
  @Test
  public void testProcedureInstances () throws Exception {
    HttpResponse response = deploy(WordCount.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = GatewayFastTestsSuite.doGet("/v2/apps/WordCount/procedures/RetrieveCounts/instances");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    String s = EntityUtils.toString(response.getEntity());
    Map<String, String> result = new Gson().fromJson(s, new TypeToken<Map<String, String>>(){}.getType());
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(1, Integer.parseInt(result.get("instances")));

    JsonObject json = new JsonObject();
    json.addProperty("instances", 10);

    response = GatewayFastTestsSuite.doPut("/v2/apps/WordCount/procedures/RetrieveCounts/instances",
                                           json.toString());
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = GatewayFastTestsSuite.doGet("/v2/apps/WordCount/procedures/RetrieveCounts/instances");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    s = EntityUtils.toString(response.getEntity());
    result = new Gson().fromJson(s, new TypeToken<Map<String, String>>(){}.getType());
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(10, Integer.parseInt(result.get("instances")));

  }

  /**
   * Tests history of a flow.
   */
  @Test
  public void testFlowHistory() throws Exception {
    try {
      HttpResponse response = deploy(WordCount.class);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      Assert.assertEquals(200,
                          GatewayFastTestsSuite.doPost("/v2/apps/WordCount/flows/WordCounter/start", null)
                            .getStatusLine().getStatusCode());
      Assert.assertEquals(200,
                          GatewayFastTestsSuite.doPost("/v2/apps/WordCount/flows/WordCounter/stop", null)
                            .getStatusLine().getStatusCode());
      Assert.assertEquals(200,
                          GatewayFastTestsSuite.doPost("/v2/apps/WordCount/flows/WordCounter/start", null)
                            .getStatusLine().getStatusCode());
      Assert.assertEquals(200,
                          GatewayFastTestsSuite.doPost("/v2/apps/WordCount/flows/WordCounter/stop", null)
                            .getStatusLine().getStatusCode());
      Assert.assertEquals(200, GatewayFastTestsSuite.doDelete("/v2/apps/WordCount").getStatusLine().getStatusCode());

      response = GatewayFastTestsSuite.doGet("/v2/apps/WordCount/flows/WordCounter/history");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      String s = EntityUtils.toString(response.getEntity());
      List<Map<String, String>> o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>(){}.getType());

      // We started and stopped twice, so we should have 2 entries.
      Assert.assertTrue(o.size() >= 2);

      // For each one, we have 4 fields.
      for (Map<String, String> m : o) {
        Assert.assertEquals(4, m.size());
      }
    } finally {
      Assert.assertEquals(200, GatewayFastTestsSuite.doDelete("/v2/apps").getStatusLine().getStatusCode());
    }
  }

  @Test
  public void testSetGetFlowletInstances() throws Exception {

  }

  /**
   * Tests specification API for a flow.
   */
  @Test
  public void testRunnableSpecification() throws Exception {
    try {
      HttpResponse response = deploy(WordCount.class);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      response = GatewayFastTestsSuite.doGet("/v2/apps/WordCount/flows/WordCounter");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      String s = EntityUtils.toString(response.getEntity());
      Assert.assertNotNull(s);
    } finally {
      Assert.assertEquals(200, GatewayFastTestsSuite.doDelete("/v2/apps").getStatusLine().getStatusCode());
    }
  }

  @Test
  public void testGetMetadata() throws Exception {
    HttpResponse response = deploy(WordCount.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = deploy(WCount.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = deploy(AppWithWorkflow.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    // verify apps
    response = GatewayFastTestsSuite.doGet("/v2/apps");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals(3, o.size());
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "App", "id", "WCount", "name", "WCount",
                                                 "description", "another Word Count Application")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "App", "id", "WordCount", "name", "WordCount",
                                                 "description", "Example Word Count Application")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "App", "id", "AppWithWorkflow", "name",
                                                 "AppWithWorkflow", "description", "Sample application")));

    // verify a single app
    response = GatewayFastTestsSuite.doGet("/v2/apps/WordCount");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    Map<String, String> app = new Gson().fromJson(s, new TypeToken<Map<String, String>>() {}.getType());
    Assert.assertEquals(ImmutableMap.of("type", "App", "id", "WordCount", "name", "WordCount",
                                        "description", "Example Word Count Application"), app);

    // verify flows
    response = GatewayFastTestsSuite.doGet("/v2/flows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals(3, o.size());
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WCount", "id", "WCounter",
                                                 "name", "WCounter", "description", "Another Word Count Flow")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WCount", "id", "WordCounter", "name",
                                                 "WordCounter", "description", "Example Word Count Flow")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WordCount", "id", "WordCounter", "name",
                                                 "WordCounter", "description", "Example Word Count Flow")));

    // verify flows by app
    response = GatewayFastTestsSuite.doGet("/v2/apps/WCount/flows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals(2, o.size());
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WCount", "id", "WCounter", "name",
                                                 "WCounter", "description", "Another Word Count Flow")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WCount", "id", "WordCounter", "name",
                                                 "WordCounter", "description", "Example Word Count Flow")));

    // verify single flow
    response = GatewayFastTestsSuite.doGet("/v2/apps/WCount/flows/WordCounter");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    Assert.assertNotNull(s);
    Assert.assertTrue(s.contains("WordCounter"));

    // verify procedures
    response = GatewayFastTestsSuite.doGet("/v2/procedures");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals(2, o.size());
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Procedure", "app", "WCount", "id", "RCounts",
                                                 "name", "RCounts", "description", "retrieve word counts")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Procedure", "app", "WordCount", "id", "RetrieveCounts",
                                                 "name", "RetrieveCounts", "description", "retrieve word counts")));

    // verify procedures by app
    response = GatewayFastTestsSuite.doGet("/v2/apps/WordCount/procedures");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals(1, o.size());
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Procedure", "app", "WordCount", "id", "RetrieveCounts",
                                                 "name", "RetrieveCounts", "description", "retrieve word counts")));


    // verify single procedure
    response = GatewayFastTestsSuite.doGet("/v2/apps/WCount/procedures/RCounts");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    Assert.assertNotNull(s);
    Assert.assertTrue(s.contains("RCounts"));

    // verify mapreduces
    response = GatewayFastTestsSuite.doGet("/v2/mapreduce");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals(1, o.size());
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Mapreduce", "app", "WCount", "id", "ClassicWordCount",
                                                 "name", "ClassicWordCount",
                                                 "description", "WordCount job from Hadoop examples")));

    // verify mapreduces by app
    response = GatewayFastTestsSuite.doGet("/v2/apps/WCount/mapreduce");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals(1, o.size());
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Mapreduce", "app", "WCount", "id", "ClassicWordCount",
                                                 "name", "ClassicWordCount",
                                                 "description", "WordCount job from Hadoop examples")));

    // verify single mapreduce
    response = GatewayFastTestsSuite.doGet("/v2/apps/WCount/mapreduce/ClassicWordCount");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    Assert.assertNotNull(s);
    Assert.assertTrue(s.contains("ClassicWordCount"));

    // verify workflows
    response = GatewayFastTestsSuite.doGet("/v2/workflows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals(1, o.size());
    Assert.assertTrue(o.contains(ImmutableMap.of(
      "type", "Workflow", "app", "AppWithWorkflow", "id", "SampleWorkflow",
      "name", "SampleWorkflow", "description",  "SampleWorkflow description")));

    // verify workflows by app
    response = GatewayFastTestsSuite.doGet("/v2/apps/AppWithWorkflow/workflows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals(1, o.size());
    Assert.assertTrue(o.contains(ImmutableMap.of(
      "type", "Workflow", "app", "AppWithWorkflow", "id", "SampleWorkflow",
      "name", "SampleWorkflow", "description",  "SampleWorkflow description")));

    // verify single workflow
    response = GatewayFastTestsSuite.doGet("/v2/apps/AppWithWorkflow/workflows/SampleWorkflow");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    Assert.assertNotNull(s);
    Assert.assertTrue(s.contains("SampleWorkflow"));

    // verify programs by non-existent app
    response = GatewayFastTestsSuite.doGet("/v2/apps/NonExistenyApp/flows");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
    response = GatewayFastTestsSuite.doGet("/v2/apps/NonExistenyApp/procedures");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
    response = GatewayFastTestsSuite.doGet("/v2/apps/NonExistenyApp/mapreduce");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
    response = GatewayFastTestsSuite.doGet("/v2/apps/NonExistenyApp/workflows");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    // verify programs by app that does not have that program type
    response = GatewayFastTestsSuite.doGet("/v2/apps/AppWithWorkflow/flows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertTrue(o.isEmpty());
    response = GatewayFastTestsSuite.doGet("/v2/apps/AppWithWorkflow/procedures");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertTrue(o.isEmpty());
    response = GatewayFastTestsSuite.doGet("/v2/apps/WordCount/mapreduce");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertTrue(o.isEmpty());
    response = GatewayFastTestsSuite.doGet("/v2/apps/WCount/workflows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertTrue(o.isEmpty());

    // verify flows by stream
    response = GatewayFastTestsSuite.doGet("/v2/streams/wordStream/flows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals(2, o.size());
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WCount", "id", "WordCounter", "name",
                                                 "WordCounter", "description", "Example Word Count Flow")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WordCount", "id", "WordCounter", "name",
                                                 "WordCounter", "description", "Example Word Count Flow")));

    // verify flows by dataset
    response = GatewayFastTestsSuite.doGet("/v2/datasets/wordStats/flows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals(2, o.size());
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WCount", "id", "WordCounter", "name",
                                                 "WordCounter", "description", "Example Word Count Flow")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WordCount", "id", "WordCounter", "name",
                                                 "WordCounter", "description", "Example Word Count Flow")));

    // verify one dataset
    response = GatewayFastTestsSuite.doGet("/v2/datasets/uniqueCount");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    Map<String, String> map = new Gson().fromJson(s, new TypeToken<Map<String, String>>() {}.getType());
    Assert.assertNotNull(map);
    Assert.assertEquals("uniqueCount", map.get("id"));
    Assert.assertEquals("uniqueCount", map.get("name"));
    Assert.assertEquals(UniqueCountTable.class.getName(), map.get("classname"));
    Assert.assertNotNull(map.get("specification"));
    DataSetSpecification spec = new Gson().fromJson(map.get("specification"), DataSetSpecification.class);
    Assert.assertNotNull(spec);

    // verify all datasets
    response = GatewayFastTestsSuite.doGet("/v2/datasets");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals(8, o.size());
    Map<String, String> expectedDataSets = ImmutableMap.<String, String>builder()
                                                       .put("input", ObjectStore.class.getName())
                                                       .put("output", ObjectStore.class.getName())
                                                       .put("wordStats", Table.class.getName())
                                                       .put("wordCounts", KeyValueTable.class.getName())
                                                       .put("uniqueCount", UniqueCountTable.class.getName())
                                                       .put("wordAssocs", AssociationTable.class.getName())
                                                       .put("jobConfig", KeyValueTable.class.getName())
                                                       .put("stats", Table.class.getName()).build();
    for (Map<String, String> ds : o) {
      Assert.assertTrue("problem with dataset " + ds.get("id"), ds.containsKey("id"));
      Assert.assertTrue("problem with dataset " + ds.get("id"), ds.containsKey("name"));
      Assert.assertTrue("problem with dataset " + ds.get("id"), ds.containsKey("classname"));
      Assert.assertTrue("problem with dataset " + ds.get("id"), expectedDataSets.containsKey(ds.get("id")));
      Assert.assertEquals("problem with dataset " + ds.get("id"),
                          expectedDataSets.get(ds.get("id")), ds.get("classname"));
    }

    // verify datasets by app
    response = GatewayFastTestsSuite.doGet("/v2/apps/WCount/datasets");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals(6, o.size());
    expectedDataSets = ImmutableMap.<String, String>builder()
                                   .put("stats", Table.class.getName())
                                   .put("wordStats", Table.class.getName())
                                   .put("wordCounts", KeyValueTable.class.getName())
                                   .put("uniqueCount", UniqueCountTable.class.getName())
                                   .put("wordAssocs", AssociationTable.class.getName())
                                   .put("jobConfig", KeyValueTable.class.getName()).build();
    for (Map<String, String> ds : o) {
      Assert.assertTrue("problem with dataset " + ds.get("id"), ds.containsKey("id"));
      Assert.assertTrue("problem with dataset " + ds.get("id"), ds.containsKey("name"));
      Assert.assertTrue("problem with dataset " + ds.get("id"), ds.containsKey("classname"));
      Assert.assertTrue("problem with dataset " + ds.get("id"), expectedDataSets.containsKey(ds.get("id")));
      Assert.assertEquals("problem with dataset " + ds.get("id"),
                          expectedDataSets.get(ds.get("id")), ds.get("classname"));
    }

    // verify one stream
    response = GatewayFastTestsSuite.doGet("/v2/streams/words");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    map = new Gson().fromJson(s, new TypeToken<Map<String, String>>() {}.getType());
    Assert.assertNotNull(map);
    Assert.assertEquals("words", map.get("id"));
    Assert.assertEquals("words", map.get("name"));
    Assert.assertNotNull(map.get("specification"));
    StreamSpecification sspec = new Gson().fromJson(map.get("specification"), StreamSpecification.class);
    Assert.assertNotNull(sspec);

    // verify all streams
    response = GatewayFastTestsSuite.doGet("/v2/streams");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals(2, o.size());
    Set<String> expectedStreams = ImmutableSet.of("words", "wordStream");
    for (Map<String, String> stream : o) {
      Assert.assertTrue("problem with stream " + stream.get("id"), stream.containsKey("id"));
      Assert.assertTrue("problem with stream " + stream.get("id"), stream.containsKey("name"));
      Assert.assertTrue("problem with dataset " + stream.get("id"), expectedStreams.contains(stream.get("id")));
    }

    // verify streams by app
    response = GatewayFastTestsSuite.doGet("/v2/apps/WCount/streams");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals(2, o.size());
    expectedStreams = ImmutableSet.of("words", "wordStream");
    for (Map<String, String> stream : o) {
      Assert.assertTrue("problem with stream " + stream.get("id"), stream.containsKey("id"));
      Assert.assertTrue("problem with stream " + stream.get("id"), stream.containsKey("name"));
      Assert.assertTrue("problem with dataset " + stream.get("id"), expectedStreams.contains(stream.get("id")));
    }

    // verify streams by app
    response = GatewayFastTestsSuite.doGet("/v2/apps/WordCount/streams");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals(1, o.size());
    expectedStreams = ImmutableSet.of("wordStream");
    for (Map<String, String> stream : o) {
      Assert.assertTrue("problem with stream " + stream.get("id"), stream.containsKey("id"));
      Assert.assertTrue("problem with stream " + stream.get("id"), stream.containsKey("name"));
      Assert.assertTrue("problem with dataset " + stream.get("id"), expectedStreams.contains(stream.get("id")));
    }
  }

  /**
   * Test for resetting app.
   */
  @Test
  public void testUnRecoverableReset() throws Exception {
    try {
      HttpResponse response = deploy(WordCount.class);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      response = GatewayFastTestsSuite.doPost("/v2/unrecoverable/reset", "");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    } finally {
      Assert.assertEquals(200, GatewayFastTestsSuite.doDelete("/v2/apps").getStatusLine().getStatusCode());
    }
    // make sure that after reset (no apps), list apps returns empty, and not 404
    Assert.assertEquals(200, GatewayFastTestsSuite.doGet("/v2/apps").getStatusLine().getStatusCode());
  }

  /**
   * Test for schedule handlers
   */
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

    response = GatewayFastTestsSuite.doGet("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/schedules");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String json = EntityUtils.toString(response.getEntity());
    List<String> schedules = new Gson().fromJson(json, new TypeToken<List<String>>(){}.getType());
    Assert.assertEquals(1, schedules.size());
    String scheduleId = schedules.get(0);
    Assert.assertNotNull(scheduleId);
    Assert.assertFalse(scheduleId.isEmpty());

    TimeUnit.SECONDS.sleep(5);
    response = GatewayFastTestsSuite.doGet("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/history");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    json = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> history = new Gson().fromJson(json,
                                                            new TypeToken<List<Map<String, String>>>(){}.getType());

    int workflowRuns = history.size();
    Assert.assertTrue(workflowRuns >= 1);

    //Check suspend status
    String scheduleStatus = String.format("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/schedules/%s/status",
                                          scheduleId);
    response = GatewayFastTestsSuite.doGet(scheduleStatus);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    json = EntityUtils.toString(response.getEntity());
    Map<String, String> output = new Gson().fromJson(json, new TypeToken<Map<String, String>>() {}.getType());
    Assert.assertEquals("SCHEDULED", output.get("status"));

    String scheduleSuspend = String.format("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/schedules/%s/suspend",
                                           scheduleId);

    response = GatewayFastTestsSuite.doPost(scheduleSuspend, "");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    //check paused state
    scheduleStatus = String.format("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/schedules/%s/status", scheduleId);
    response = GatewayFastTestsSuite.doGet(scheduleStatus);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    json = EntityUtils.toString(response.getEntity());
    output = new Gson().fromJson(json, new TypeToken<Map<String, String>>() {}.getType());
    Assert.assertEquals("SUSPENDED", output.get("status"));

    TimeUnit.SECONDS.sleep(2); //wait till any running jobs just before suspend call completes.

    response = GatewayFastTestsSuite.doGet("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/history");
    json = EntityUtils.toString(response.getEntity());
    history = new Gson().fromJson(json,
                                  new TypeToken<List<Map<String, String>>>(){}.getType());
    workflowRuns = history.size();

    //Sleep for some time and verify there are no more scheduled jobs after the suspend.
    TimeUnit.SECONDS.sleep(10);

    response = GatewayFastTestsSuite.doGet("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/history");
    json = EntityUtils.toString(response.getEntity());
    history = new Gson().fromJson(json,
                                  new TypeToken<List<Map<String, String>>>(){}.getType());
    int workflowRunsAfterSuspend = history.size();
    Assert.assertEquals(workflowRuns, workflowRunsAfterSuspend);

    String scheduleResume = String.format("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/schedules/%s/resume",
                                          scheduleId);

    response = GatewayFastTestsSuite.doPost(scheduleResume, "");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    //Sleep for some time and verify there are no more scheduled jobs after the pause.
    TimeUnit.SECONDS.sleep(3);
    response = GatewayFastTestsSuite.doGet("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/history");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    json = EntityUtils.toString(response.getEntity());
    history = new Gson().fromJson(json,
                                  new TypeToken<List<Map<String, String>>>(){}.getType());

    int workflowRunsAfterResume = history.size();
    //Verify there is atleast one run after the pause
    Assert.assertTrue(workflowRunsAfterResume > workflowRunsAfterSuspend + 1);

    //check scheduled state
    scheduleStatus = String.format("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/schedules/%s/status", scheduleId);
    response = GatewayFastTestsSuite.doGet(scheduleStatus);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    json = EntityUtils.toString(response.getEntity());
    output = new Gson().fromJson(json, new TypeToken<Map<String, String>>() {}.getType());
    Assert.assertEquals("SCHEDULED", output.get("status"));

    //Check status of a non existing schedule
    String notFoundSchedule = String.format("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/schedules/%s/status",
                                           "invalidId");

    response = GatewayFastTestsSuite.doGet(notFoundSchedule);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    json = EntityUtils.toString(response.getEntity());
    output = new Gson().fromJson(json, new TypeToken<Map<String, String>>() {}.getType());
    Assert.assertEquals("NOT_FOUND", output.get("status"));
  }

  @Test
  public void testWorkflowRuntimeArgs() throws Exception {
    HttpResponse response = deploy(AppWithWorkflow.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Map<String, String> args = Maps.newHashMap();
    args.put("Key1", "Val1");
    args.put("Key2", "Val1");
    args.put("Key2", "Val1");

    String argString = GSON.toJson(args, new TypeToken<Map<String, String>>(){}.getType());
    response = GatewayFastTestsSuite.doPut("/v2/apps/AppWithWorkflows/workflows/SampleWorkflow/runtimeargs",
                                            argString);

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = GatewayFastTestsSuite.doGet("/v2/apps/AppWithWorkflows/workflows/SampleWorkflow/runtimeargs");

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Map<String, String> argsRead = GSON.fromJson(EntityUtils.toString(response.getEntity()),
                                                 new TypeToken<Map<String, String>>(){}.getType());

    Assert.assertEquals(args.size(), argsRead.size());

    for (Map.Entry<String, String> entry : args.entrySet()){
       Assert.assertEquals(entry.getValue(), argsRead.get(entry.getKey()));
    }

    //test empty runtime args
    response = GatewayFastTestsSuite.doPut("/v2/apps/AppWithWorkflows/workflows/SampleWorkflow/runtimeargs", "");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = GatewayFastTestsSuite.doGet("/v2/apps/AppWithWorkflows/workflows/SampleWorkflow/runtimeargs");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    argsRead = GSON.fromJson(EntityUtils.toString(response.getEntity()),
                             new TypeToken<Map<String, String>>(){}.getType());
    Assert.assertEquals(0, argsRead.size());

    //test null runtime args
    response = GatewayFastTestsSuite.doPut("/v2/apps/AppWithWorkflows/workflows/SampleWorkflow/runtimeargs", null);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = GatewayFastTestsSuite.doGet("/v2/apps/AppWithWorkflows/workflows/SampleWorkflow/runtimeargs");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    argsRead = GSON.fromJson(EntityUtils.toString(response.getEntity()),
                             new TypeToken<Map<String, String>>(){}.getType());
    Assert.assertEquals(0, argsRead.size());
  }



  @Test
  public void testFlowRuntimeArgs() throws Exception {
    HttpResponse response = deploy(WordCount.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Map<String, String> args = Maps.newHashMap();
    args.put("Key1", "Val1");
    args.put("Key2", "Val1");
    args.put("Key2", "Val1");

    String argString = GSON.toJson(args, new TypeToken<Map<String, String>>(){}.getType());
    response = GatewayFastTestsSuite.doPut("/v2/apps/WordCount/flows/WordCounter/runtimeargs",
                                            argString);

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = GatewayFastTestsSuite.doGet("/v2/apps/WordCount/flows/WordCounter/runtimeargs");

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Map<String, String> argsRead = GSON.fromJson(EntityUtils.toString(response.getEntity()),
                                                 new TypeToken<Map<String, String>>(){}.getType());

    Assert.assertEquals(args.size(), argsRead.size());

    for (Map.Entry<String, String> entry : args.entrySet()){
      Assert.assertEquals(entry.getValue(), argsRead.get(entry.getKey()));
    }

    //test empty runtime args
    response = GatewayFastTestsSuite.doPut("/v2/apps/WordCount/flows/WordCounter/runtimeargs", "");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = GatewayFastTestsSuite.doGet("/v2/apps/WordCount/flows/WordCounter/runtimeargs");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    argsRead = GSON.fromJson(EntityUtils.toString(response.getEntity()),
                             new TypeToken<Map<String, String>>(){}.getType());
    Assert.assertEquals(0, argsRead.size());

    //test null runtime args
    response = GatewayFastTestsSuite.doPut("/v2/apps/WordCount/flows/WordCounter/runtimeargs", null);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = GatewayFastTestsSuite.doGet("/v2/apps/WordCount/flows/WordCounter/runtimeargs");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    argsRead = GSON.fromJson(EntityUtils.toString(response.getEntity()),
                             new TypeToken<Map<String, String>>(){}.getType());
    Assert.assertEquals(0, argsRead.size());
  }
}
