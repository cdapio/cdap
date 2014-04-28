package com.continuuity.gateway.handlers;

import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.app.program.ManifestFields;
import com.continuuity.common.conf.Constants;
import com.continuuity.gateway.GatewayFastTestsSuite;
import com.continuuity.gateway.apps.wordcount.AppWithWorkflow;
import com.continuuity.gateway.apps.wordcount.AssociationTable;
import com.continuuity.gateway.apps.wordcount.UniqueCountTable;
import com.continuuity.gateway.apps.wordcount.WCount;
import com.continuuity.gateway.apps.wordcount.WordCount;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.apache.twill.internal.utils.Dependencies;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;

/**
 * Testing of App Fabric REST Endpoints.
 */
public class AppFabricServiceHandlerTest {

  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type LIST_MAP_STRING_STRING_TYPE = new TypeToken<List<Map<String, String>>>() { }.getType();

  /**
   * Deploys and application.
   */
  static HttpResponse deploy(Class<?> application) throws Exception {
    return deploy(application, null);
  }
  /**
   * Deploys and application with (optionally) defined app name.
   */
  static HttpResponse deploy(Class<?> application, @Nullable String appName) throws Exception {
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
      request = GatewayFastTestsSuite.getPost("/v2/apps");
    } else {
      request = GatewayFastTestsSuite.getPut("/v2/apps/" + appName);
    }
    request.setHeader(Constants.Gateway.CONTINUUITY_API_KEY, "api-key-example");
    request.setHeader("X-Archive-Name", application.getSimpleName() + ".jar");
    request.setEntity(new ByteArrayEntity(bos.toByteArray()));
    return GatewayFastTestsSuite.execute(request);
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
   * Tests deploying an application with name overriding.
   */
  @Test
  public void testDeployWithName() throws Exception {
    // deploying app
    HttpResponse response = deploy(WordCount.class, "app1");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    // deploying again with different name
    response = deploy(WordCount.class, "app2");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    List<String> apps = getDeployedApps();
    Assert.assertTrue(apps.contains("app1"));
    Assert.assertTrue(apps.contains("app2"));
  }

  private List<String> getDeployedApps() throws Exception {
    HttpResponse appsResponse = GatewayFastTestsSuite.doGet("/v2/apps");
    Assert.assertEquals(200, appsResponse.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(appsResponse.getEntity());
    List<Map<String, String>> map = GSON.fromJson(s, LIST_MAP_STRING_STRING_TYPE);
    List<String> result = Lists.newArrayList();
    for (Map<String, String> app : map) {
      result.add(app.get("id"));
    }
    return result;
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
    // todo: we don't have delete all apps now: it was a "reset" functionality that does nothing now (deprecated)
    //       So, even though it returns 200 it does nothing. Remove it when reset method is gone
//    Assert.assertEquals(0, getDeployedApps().size());
  }

  /**
   * @return Runnable status
   */
  private String getRunnableStatus(String runnableType, String appId, String runnableId) throws Exception {
    HttpResponse response =
      GatewayFastTestsSuite.doGet("/v2/apps/" + appId + "/" + runnableType + "/" + runnableId + "/status");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Map<String, String> o = new Gson().fromJson(s, MAP_STRING_STRING_TYPE);
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
    Map<String, String> o = new Gson().fromJson(s, MAP_STRING_STRING_TYPE);
    return o.get("status");
  }

  /**
   * Tests deploying a flow, starting a flow, stopping a flow, test status of non existing flow
   * and deleting the application. Also tests that the reactor cannot be reset when the flow is running.
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
      Assert.assertEquals(403,
                          GatewayFastTestsSuite.doDelete("/v2/apps/WordCount/flows/WordCounter/queues")
                                               .getStatusLine().getStatusCode()
      );
      // attempt to reset the reactor, should return bad request including an error message
      response = GatewayFastTestsSuite.doPost("/v2/unrecoverable/reset", "");
      Assert.assertEquals(400, response.getStatusLine().getStatusCode());
      Assert.assertNotNull(response.getEntity());
      Assert.assertTrue(response.getEntity().getContentLength() > 0);
    } finally {
      Assert.assertEquals(200,
                          GatewayFastTestsSuite.doPost("/v2/apps/WordCount/flows/WordCounter/stop", null)
                            .getStatusLine().getStatusCode()
      );
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
   * Test the behavior when trying to start/stop flow, procedure, etc. that are not deployed.
   */
  @Test
  public void testNotDeployedStartStop() throws Exception {
    // Make sure app is deleted
    GatewayFastTestsSuite.doDelete("/v2/apps/WordCount");

    // Try starting flow/procedure of the app
    Assert.assertEquals(404,
                        GatewayFastTestsSuite.doPost("/v2/apps/WordCount/flows/WordCounter/start", null)
                          .getStatusLine().getStatusCode()
    );
    Assert.assertEquals(404,
                        GatewayFastTestsSuite.doPost("/v2/apps/WordCount/procedures/RetrieveCounts/start", null)
                          .getStatusLine().getStatusCode()
    );

    // Stopping should return 404 too.
    Assert.assertEquals(404,
                        GatewayFastTestsSuite.doPost("/v2/apps/WordCount/flows/WordCounter/stop", null)
                          .getStatusLine().getStatusCode()
    );
    Assert.assertEquals(404,
                        GatewayFastTestsSuite.doPost("/v2/apps/WordCount/procedures/RetrieveCounts/stop", null)
                          .getStatusLine().getStatusCode()
    );

    // Now deploy the app
    Assert.assertEquals(200, deploy(WordCount.class).getStatusLine().getStatusCode());

    // Starting should now work fine
    Assert.assertEquals(200,
                        GatewayFastTestsSuite.doPost("/v2/apps/WordCount/flows/WordCounter/start", null)
                          .getStatusLine().getStatusCode()
    );
    Assert.assertEquals(200,
                        GatewayFastTestsSuite.doPost("/v2/apps/WordCount/procedures/RetrieveCounts/start", null)
                          .getStatusLine().getStatusCode()
    );

    // Starting again should throw exception
    Assert.assertEquals(409,
                        GatewayFastTestsSuite.doPost("/v2/apps/WordCount/flows/WordCounter/start", null)
                          .getStatusLine().getStatusCode()
    );
    Assert.assertEquals(409,
                        GatewayFastTestsSuite.doPost("/v2/apps/WordCount/procedures/RetrieveCounts/start", null)
                          .getStatusLine().getStatusCode()
    );

    // Stopping flow and procedure.
    Assert.assertEquals(200,
                        GatewayFastTestsSuite.doPost("/v2/apps/WordCount/flows/WordCounter/stop", null)
                          .getStatusLine().getStatusCode()
    );
    Assert.assertEquals(200,
                        GatewayFastTestsSuite.doPost("/v2/apps/WordCount/procedures/RetrieveCounts/stop", null)
                          .getStatusLine().getStatusCode()
    );

    // Stopping again should throw exception.
    Assert.assertEquals(409,
                        GatewayFastTestsSuite.doPost("/v2/apps/WordCount/flows/WordCounter/stop", null)
                          .getStatusLine().getStatusCode()
    );
    Assert.assertEquals(409,
                        GatewayFastTestsSuite.doPost("/v2/apps/WordCount/procedures/RetrieveCounts/stop", null)
                          .getStatusLine().getStatusCode()
    );

    // Delete app
    Assert.assertEquals(200, GatewayFastTestsSuite.doDelete("/v2/apps/WordCount").getStatusLine().getStatusCode());
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
    Map<String, String> result = new Gson().fromJson(s, MAP_STRING_STRING_TYPE);
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
    result = new Gson().fromJson(s, MAP_STRING_STRING_TYPE);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(10, Integer.parseInt(result.get("instances")));

  }

  @Test
  public void testSetGetFlowletInstances() throws Exception {

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
    List<Map<String, String>> o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
    Assert.assertEquals(5, o.size());
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "App", "id", "WCount", "name", "WCount",
                                                 "description", "another Word Count Application")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "App", "id", "WordCount", "name", "WordCount",
                                                 "description", "Example Word Count Application")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "App", "id", "app1", "name", "app1",
                                                 "description", "Example Word Count Application")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "App", "id", "app2", "name", "app2",
                                                 "description", "Example Word Count Application")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "App", "id", "AppWithWorkflow", "name",
                                                 "AppWithWorkflow", "description", "Sample application")));

    // verify a single app
    response = GatewayFastTestsSuite.doGet("/v2/apps/WordCount");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    Map<String, String> app = new Gson().fromJson(s, MAP_STRING_STRING_TYPE);
    Assert.assertEquals(ImmutableMap.of("type", "App", "id", "WordCount", "name", "WordCount",
                                        "description", "Example Word Count Application"), app);

    // verify flows
    response = GatewayFastTestsSuite.doGet("/v2/flows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
    Assert.assertEquals(5, o.size());
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WCount", "id", "WCounter",
                                                 "name", "WCounter", "description", "Another Word Count Flow")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WCount", "id", "WordCounter", "name",
                                                 "WordCounter", "description", "Example Word Count Flow")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WordCount", "id", "WordCounter", "name",
                                                 "WordCounter", "description", "Example Word Count Flow")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "app1", "id", "WordCounter", "name",
                                                 "WordCounter", "description", "Example Word Count Flow")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "app2", "id", "WordCounter", "name",
                                                 "WordCounter", "description", "Example Word Count Flow")));

    // verify flows by app
    response = GatewayFastTestsSuite.doGet("/v2/apps/WCount/flows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
    Assert.assertEquals(2, o.size());
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WCount", "id", "WCounter", "name",
                                                 "WCounter", "description", "Another Word Count Flow")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WCount", "id", "WordCounter", "name",
                                                 "WordCounter", "description", "Example Word Count Flow")));


    // verify procedures
    response = GatewayFastTestsSuite.doGet("/v2/procedures");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
    Assert.assertEquals(4, o.size());
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Procedure", "app", "WCount", "id", "RCounts",
                                                 "name", "RCounts", "description", "retrieve word counts")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Procedure", "app", "WordCount", "id", "RetrieveCounts",
                                                 "name", "RetrieveCounts", "description", "retrieve word counts")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Procedure", "app", "app1", "id", "RetrieveCounts",
                                                 "name", "RetrieveCounts", "description", "retrieve word counts")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Procedure", "app", "app2", "id", "RetrieveCounts",
                                                 "name", "RetrieveCounts", "description", "retrieve word counts")));

    // verify procedures by app
    response = GatewayFastTestsSuite.doGet("/v2/apps/WordCount/procedures");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
    Assert.assertEquals(1, o.size());
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Procedure", "app", "WordCount", "id", "RetrieveCounts",
                                                 "name", "RetrieveCounts", "description", "retrieve word counts")));

    // verify mapreduces
    response = GatewayFastTestsSuite.doGet("/v2/mapreduce");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
    Assert.assertEquals(1, o.size());
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Mapreduce", "app", "WCount", "id", "ClassicWordCount",
                                                 "name", "ClassicWordCount",
                                                 "description", "WordCount job from Hadoop examples")));


    // verify workflows
    response = GatewayFastTestsSuite.doGet("/v2/workflows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
    Assert.assertEquals(1, o.size());
    Assert.assertTrue(o.contains(ImmutableMap.of(
      "type", "Workflow", "app", "AppWithWorkflow", "id", "SampleWorkflow",
      "name", "SampleWorkflow", "description",  "SampleWorkflow description")));


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
    o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
    Assert.assertTrue(o.isEmpty());
    response = GatewayFastTestsSuite.doGet("/v2/apps/AppWithWorkflow/procedures");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
    Assert.assertTrue(o.isEmpty());
    response = GatewayFastTestsSuite.doGet("/v2/apps/WordCount/mapreduce");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
    Assert.assertTrue(o.isEmpty());
    response = GatewayFastTestsSuite.doGet("/v2/apps/WCount/workflows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
    Assert.assertTrue(o.isEmpty());

    // verify flows by stream
    response = GatewayFastTestsSuite.doGet("/v2/streams/wordStream/flows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
    Assert.assertEquals(4, o.size());
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WCount", "id", "WordCounter", "name",
                                                 "WordCounter", "description", "Example Word Count Flow")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WordCount", "id", "WordCounter", "name",
                                                 "WordCounter", "description", "Example Word Count Flow")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "app1", "id", "WordCounter", "name",
                                                 "WordCounter", "description", "Example Word Count Flow")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "app2", "id", "WordCounter", "name",
                                                 "WordCounter", "description", "Example Word Count Flow")));

    // verify flows by dataset
    response = GatewayFastTestsSuite.doGet("/v2/datasets/wordStats/flows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
    Assert.assertEquals(4, o.size());
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WCount", "id", "WordCounter", "name",
                                                 "WordCounter", "description", "Example Word Count Flow")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WordCount", "id", "WordCounter", "name",
                                                 "WordCounter", "description", "Example Word Count Flow")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "app1", "id", "WordCounter", "name",
                                                 "WordCounter", "description", "Example Word Count Flow")));
    Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "app2", "id", "WordCounter", "name",
                                                 "WordCounter", "description", "Example Word Count Flow")));

    // verify one dataset
    response = GatewayFastTestsSuite.doGet("/v2/datasets/uniqueCount");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    s = EntityUtils.toString(response.getEntity());
    Map<String, String> map = new Gson().fromJson(s, MAP_STRING_STRING_TYPE);
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
    o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
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
    o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
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
    map = new Gson().fromJson(s, MAP_STRING_STRING_TYPE);
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
    o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
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
    o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
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
    o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
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
}
