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

import co.cask.cdap.AppWithDataset;
import co.cask.cdap.AppWithDatasetDuplicate;
import co.cask.cdap.AppWithNoServices;
import co.cask.cdap.BloatedWordCountApp;
import co.cask.cdap.ConfigTestApp;
import co.cask.cdap.WordCountApp;
import co.cask.cdap.api.Config;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.AppLifecycleHttpHandler;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Tests for {@link AppLifecycleHttpHandler}
 */
public class AppLifecycleHttpHandlerTest extends AppFabricTestBase {

  /**
   * Tests deploying an application in a non-existing non-default namespace.
   */
  @Test
  public void testDeployNonExistingNamespace() throws Exception {
    HttpResponse response = deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, "random");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
    NotFoundException nfe = new NamespaceNotFoundException(Id.Namespace.from("random"));
    Assert.assertEquals(nfe.getMessage(), readResponse(response));
  }

  /**
   * Tests deploying an application.
   */
  @Test
  public void testDeployValid() throws Exception {
    HttpResponse response = deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testDeployWithExtraConfig() throws Exception {
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "extraConfig", "1.0.0-SNAPSHOT");
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "ExtraConfigApp");
    HttpResponse response = addAppArtifact(artifactId, AppWithNoServices.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = deploy(appId, new AppRequest<>(ArtifactSummary.from(artifactId), new ExtraConfig()));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    deleteApp(appId, 200);
    deleteArtifact(artifactId, 200);
  }

  @Test
  public void testAppWithConfig() throws Exception {
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp");
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "appWithConfig", "1.0.0-SNAPSHOT");
    HttpResponse response = addAppArtifact(artifactId, ConfigTestApp.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("abc", "def");
    response = deploy(appId, new AppRequest<>(ArtifactSummary.from(artifactId), config));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    JsonObject appDetails = getAppDetails(Id.Namespace.DEFAULT.getId(), "ConfigApp");
    Assert.assertEquals(GSON.toJson(config), appDetails.get("configuration").getAsString());

    deleteApp(appId, 200);
    deleteArtifact(artifactId, 200);
  }

  @Test
  public void testDeployUsingNonexistantArtifact404() throws Exception {
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "badapp");
    AppRequest<Config> appRequest =
      new AppRequest<>(new ArtifactSummary("something", "1.0.0"), null);
    HttpResponse response = deploy(appId, appRequest);
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testDeployUsingArtifact() throws Exception {
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "configapp", "1.0.0");
    addAppArtifact(artifactId, ConfigTestApp.class);

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "cfgApp");
    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("abc", "def");
    AppRequest<ConfigTestApp.ConfigClass> request = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), config);
    Assert.assertEquals(200, deploy(appId, request).getStatusLine().getStatusCode());

    JsonObject appDetails = getAppDetails(Id.Namespace.DEFAULT.getId(), appId.getId());
    Assert.assertEquals(GSON.toJson(config), appDetails.get("configuration").getAsString());

    Assert.assertEquals(200,
      doDelete(getVersionedAPIPath("apps/" + appId.getId(), appId.getNamespaceId())).getStatusLine().getStatusCode());
  }

  /**
   * Tests deploying an invalid application.
   */
  @Test
  public void testDeployInvalid() throws Exception {
    HttpResponse response = deploy(String.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());
    Assert.assertNotNull(response.getEntity());
    Assert.assertTrue(response.getEntity().getContentLength() > 0);
  }

  /**
   * Tests deploying an application with dataset same name as existing dataset but a different type
   */
  @Test
  public void testDeployFailure() throws Exception {
    HttpResponse response = deploy(AppWithDataset.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertNotNull(response.getEntity());

    response = deploy(AppWithDatasetDuplicate.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());
    Assert.assertNotNull(response.getEntity());
  }

  @Test
  public void testListNonExistentNamespace() throws Exception {
    HttpResponse response = doGet(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN,
                                                      NONEXISTENT_NAMESPACE));
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testListAndGet() throws Exception {
    final String appName = "AppWithDatasetName";
    Id.Namespace ns1 = Id.Namespace.from(TEST_NAMESPACE1);
    Id.Namespace ns2 = Id.Namespace.from(TEST_NAMESPACE2);

    Id.Artifact ns2ArtifactId = Id.Artifact.from(ns2, "bloatedListAndGet", "1.0.0-SNAPSHOT");

    //deploy without name to testnamespace1
    HttpResponse response = deploy(BloatedWordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    //deploy with name to testnamespace2
    response = addAppArtifact(ns2ArtifactId, BloatedWordCountApp.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Id.Application appId = Id.Application.from(ns2, appName);
    response = deploy(appId, new AppRequest<Config>(ArtifactSummary.from(ns2ArtifactId)));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertNotNull(response.getEntity());

    //verify testnamespace1 has 1 app
    List<JsonObject> apps = getAppList(TEST_NAMESPACE1);
    Assert.assertEquals(1, apps.size());

    //verify testnamespace2 has 1 app
    apps = getAppList(TEST_NAMESPACE2);
    Assert.assertEquals(1, apps.size());

    //get and verify app details in testnamespace1
    JsonObject result = getAppDetails(TEST_NAMESPACE1, "WordCountApp");
    Assert.assertEquals("WordCountApp", result.get("name").getAsString());
    Assert.assertEquals("Application for counting words", result.get("description").getAsString());

    JsonArray streams = result.get("streams").getAsJsonArray();
    Assert.assertEquals(1, streams.size());
    JsonObject stream = streams.get(0).getAsJsonObject();
    Assert.assertEquals("text", stream.get("name").getAsString());

    JsonArray datasets = result.get("datasets").getAsJsonArray();
    Assert.assertEquals(1, datasets.size());
    JsonObject dataset = datasets.get(0).getAsJsonObject();
    Assert.assertEquals("mydataset", dataset.get("name").getAsString());

    JsonArray programs = result.get("programs").getAsJsonArray();
    Assert.assertEquals(6, programs.size());
    JsonObject[] progs = new JsonObject[programs.size()];
    for (int i = 0; i < programs.size(); i++) {
      progs[i] = programs.get(i).getAsJsonObject();
    }
    // sort the programs by name to make this test deterministic
    Arrays.sort(progs, new Comparator<JsonObject>() {
      @Override
      public int compare(JsonObject o1, JsonObject o2) {
        return o1.get("name").getAsString().compareTo(o2.get("name").getAsString());
      }
    });
    int i = 0;
    Assert.assertEquals("Worker", progs[i].get("type").getAsString());
    Assert.assertEquals("LazyGuy", progs[i].get("name").getAsString());
    Assert.assertEquals("nothing to describe", progs[i].get("description").getAsString());
    i++;
    Assert.assertEquals("Workflow", progs[i].get("type").getAsString());
    Assert.assertEquals("SingleStep", progs[i].get("name").getAsString());
    Assert.assertEquals("", progs[i].get("description").getAsString());
    i++;
    Assert.assertEquals("Spark", progs[i].get("type").getAsString());
    Assert.assertEquals("SparklingNothing", progs[i].get("name").getAsString());
    Assert.assertEquals("Spark program that does nothing", progs[i].get("description").getAsString());
    i++;
    Assert.assertEquals("Mapreduce", progs[i].get("type").getAsString());
    Assert.assertEquals("VoidMapReduceJob", progs[i].get("name").getAsString());
    Assert.assertTrue(progs[i].get("description").getAsString().startsWith("Mapreduce that does nothing"));
    i++;
    Assert.assertEquals("Flow", progs[i].get("type").getAsString());
    Assert.assertEquals("WordCountFlow", progs[i].get("name").getAsString());
    Assert.assertEquals("Flow for counting words", progs[i].get("description").getAsString());
    i++;
    Assert.assertEquals("Service", progs[i].get("type").getAsString());
    Assert.assertEquals("WordFrequencyService", progs[i].get("name").getAsString());
    Assert.assertEquals("", progs[i].get("description").getAsString());

    //get and verify app details in testnamespace2
    result = getAppDetails(TEST_NAMESPACE2, appName);
    Assert.assertEquals(appName, result.get("name").getAsString());

    //delete app in testnamespace1
    response = doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    //delete app in testnamespace2
    response = doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    deleteArtifact(ns2ArtifactId, 200);
  }

  /**
   * Tests deleting an application.
   */
  @Test
  public void testDelete() throws Exception {
    // Delete an non-existing app
    HttpResponse response = doDelete(getVersionedAPIPath("apps/XYZ", Constants.Gateway.API_VERSION_3_TOKEN,
                                                         TEST_NAMESPACE1));
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Id.Program program = Id.Program.from(TEST_NAMESPACE1, "WordCountApp", ProgramType.FLOW, "WordCountFlow");
    startProgram(program);
    waitState(program, "RUNNING");
    // Try to delete an App while its flow is running
    response = doDelete(getVersionedAPIPath("apps/WordCountApp", Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE1));
    Assert.assertEquals(403, response.getStatusLine().getStatusCode());
    Assert.assertEquals("'" + program.getApplication() +
                          "' could not be deleted. Reason: The following programs are still running: "
                          + program.getId(), readResponse(response));

    stopProgram(program);
    waitState(program, "STOPPED");

    startProgram(program);
    waitState(program, "RUNNING");
    // Try to delete all Apps while flow is running
    response = doDelete(getVersionedAPIPath("apps", Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE1));
    Assert.assertEquals(403, response.getStatusLine().getStatusCode());
    Assert.assertEquals("'" + program.getNamespace() +
                          "' could not be deleted. Reason: The following programs are still running: "
                          + program.getApplicationId() + ": " + program.getId(), readResponse(response));

    stopProgram(program);
    waitState(program, "STOPPED");

    // Delete the app in the wrong namespace
    response = doDelete(getVersionedAPIPath("apps/WordCountApp", Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE2));
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    //Delete the App after stopping the flow
    response = doDelete(getVersionedAPIPath("apps/WordCountApp/", Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doDelete(getVersionedAPIPath("apps/WordCountApp/", Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE1));
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    // deleting the app should not delete the artifact
    response = doGet(getVersionedAPIPath("artifacts/WordCountApp", Constants.Gateway.API_VERSION_3_TOKEN,
                                         TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    List<ArtifactSummary> summaries = readResponse(response, new TypeToken<List<ArtifactSummary>>() { }.getType());
    Assert.assertFalse(summaries.isEmpty());
  }

  private static class ExtraConfig extends Config {
    @SuppressWarnings("unused")
    private final int x = 5;
  }
}
