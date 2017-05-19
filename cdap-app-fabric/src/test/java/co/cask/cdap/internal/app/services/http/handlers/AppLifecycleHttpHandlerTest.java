/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.AppLifecycleHttpHandler;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.jboss.resteasy.util.HttpResponseCodes;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    NotFoundException nfe = new NamespaceNotFoundException(new NamespaceId("random"));
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
    response = deploy(appId, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId()), new ExtraConfig()));
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
    response = deploy(appId, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId()), config));
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

  @Test
  public void testOwnerUsingArtifact() throws Exception {
    ArtifactId artifactId = new ArtifactId(NamespaceId.DEFAULT.getNamespace(), "wordCountArtifact", "1.0.0");
    addAppArtifact(artifactId.toId(), WordCountApp.class);
    ApplicationId applicationId = new ApplicationId(NamespaceId.DEFAULT.getNamespace(), "WordCountApp");
    // deploy an app with a owner
    String ownerPrincipal = "alice/somehost.net@somekdc.net";
    AppRequest<ConfigTestApp.ConfigClass> appRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()), null, ownerPrincipal);
    Assert.assertEquals(HttpResponseCodes.SC_OK, deploy(applicationId, appRequest).getStatusLine().getStatusCode());

    // should be able to retrieve the owner information of the app
    JsonObject appDetails = getAppDetails(NamespaceId.DEFAULT.getNamespace(), applicationId.getApplication());
    Assert.assertEquals(ownerPrincipal, appDetails.get(Constants.Security.PRINCIPAL).getAsString());

    // the stream created by the app should have the app owner too
    Assert.assertEquals(ownerPrincipal,
                        getStreamConfig(applicationId.getNamespaceId().stream("text")).getOwnerPrincipal());

    // the dataset created by the app should have the app owner too
    Assert.assertEquals(ownerPrincipal,
                        getDatasetMeta(applicationId.getNamespaceId().dataset("mydataset")).getOwnerPrincipal());

    // trying to deploy the same app with another owner should fail
    String bobPrincipal = "bob/somehost.net@somekdc.net";
    appRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()), null, bobPrincipal);
    Assert.assertEquals(HttpResponseCodes.SC_FORBIDDEN,
                        deploy(applicationId, appRequest).getStatusLine().getStatusCode());

    // trying to deploy the same app with different version and another owner should fail too
    appRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()), null, bobPrincipal);
    Assert.assertEquals(HttpResponseCodes.SC_FORBIDDEN,
                        deploy(new ApplicationId(applicationId.getNamespace(), applicationId.getApplication(), "1.0"),
                               appRequest).getStatusLine().getStatusCode());

    // trying to re-deploy the same app with same owner should pass
    appRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()), null, ownerPrincipal);
    Assert.assertEquals(HttpResponseCodes.SC_OK,
                        deploy(applicationId, appRequest).getStatusLine().getStatusCode());

    // trying to re-deploy the same app with different version but same owner should pass
    appRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()), null, ownerPrincipal);
    Assert.assertEquals(HttpResponseCodes.SC_OK,
                        deploy(new ApplicationId(applicationId.getNamespace(), applicationId.getApplication(), "1.0"),
                               appRequest).getStatusLine().getStatusCode());

    // clean up the app
    Assert.assertEquals(200,
                        doDelete(getVersionedAPIPath("apps/" + applicationId.getApplication(),
                                                     applicationId.getNamespace())).getStatusLine().getStatusCode());

    // deletion of app should delete the stream/dataset owner information as they themselves are not deleted
    Assert.assertEquals(ownerPrincipal,
                        getStreamConfig(applicationId.getNamespaceId().stream("text")).getOwnerPrincipal());
    Assert.assertEquals(ownerPrincipal,
                        getDatasetMeta(applicationId.getNamespaceId().dataset("mydataset")).getOwnerPrincipal());

    // cleanup
    deleteNamespace(NamespaceId.DEFAULT.getNamespace());
  }

  @Test
  public void testOwnerInHeaders() throws Exception {
    String ownerPrincipal = "bob/somehost.net@somekdc.net";
    HttpResponse response = deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN,
                                   NamespaceId.DEFAULT.getNamespace(), ownerPrincipal);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    ApplicationId applicationId = new ApplicationId(NamespaceId.DEFAULT.getNamespace(), "WordCountApp");

    // should be able to retrieve the owner information of the app
    JsonObject appDetails = getAppDetails(NamespaceId.DEFAULT.getNamespace(), applicationId.getApplication());
    Assert.assertEquals(ownerPrincipal, appDetails.get(Constants.Security.PRINCIPAL).getAsString());

    // cleanup app
    Assert.assertEquals(200,
                        doDelete(getVersionedAPIPath("apps/" + applicationId.getApplication(),
                                                     applicationId.getNamespace())).getStatusLine().getStatusCode());

    // cleanup
    deleteNamespace(NamespaceId.DEFAULT.getNamespace());
  }

  @Test
  public void testDeployVersionedAndNonVersionedApp() throws Exception {
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "configapp", "1.0.0");
    addAppArtifact(artifactId, ConfigTestApp.class);

    ApplicationId appId = new ApplicationId(Id.Namespace.DEFAULT.getId(), "cfgAppWithVersion", "1.0.0");
    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("abc", "def");
    AppRequest<ConfigTestApp.ConfigClass> request = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), config);
    Assert.assertEquals(200, deploy(appId, request).getStatusLine().getStatusCode());
    // Cannot update the app created by versioned API with versionId not ending with "-SNAPSHOT"
    Assert.assertEquals(409, deploy(appId, request).getStatusLine().getStatusCode());
    Assert.assertEquals(404, getAppResponse(Id.Namespace.DEFAULT.getId(), appId.getApplication(),
                                            "non_existing_version").getStatusLine().getStatusCode());
    Assert.assertEquals(404, getAppResponse(Id.Namespace.DEFAULT.getId(),
                                            appId.getApplication()).getStatusLine().getStatusCode());

    // Deploy app with default versionId by non-versioned API
    Id.Application appIdDefault = Id.Application.from(Id.Namespace.DEFAULT, appId.getApplication());
    ConfigTestApp.ConfigClass configDefault = new ConfigTestApp.ConfigClass("uvw", "xyz");
    AppRequest<ConfigTestApp.ConfigClass> requestDefault = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), configDefault);
    Assert.assertEquals(200, deploy(appIdDefault, requestDefault).getStatusLine().getStatusCode());

    // Deploy app with versionId "version_2" by versioned API
    ApplicationId appIdV2 = new ApplicationId(appId.getNamespace(), appId.getApplication(), "2.0.0");
    ConfigTestApp.ConfigClass configV2 = new ConfigTestApp.ConfigClass("ghi", "jkl");
    AppRequest<ConfigTestApp.ConfigClass> requestV2 = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), configV2);
    Assert.assertEquals(200, deploy(appIdV2, requestV2).getStatusLine().getStatusCode());

    Set<String> versions = ImmutableSet.of("-SNAPSHOT", "2.0.0", "1.0.0");
    Assert.assertEquals(versions, getAppVersions(appId.getNamespace(), appId.getApplication()));

    List<JsonObject> appList = getAppList(appId.getNamespace());
    Set<String> receivedVersions = new HashSet<>();
    for (JsonObject appRecord : appList) {
      receivedVersions.add(appRecord.getAsJsonPrimitive("version").getAsString());
    }
    Assert.assertEquals(versions, receivedVersions);

    JsonObject appDetails = getAppDetails(appId.getNamespace(), appId.getApplication(), appId.getVersion());
    Assert.assertEquals(GSON.toJson(config), appDetails.get("configuration").getAsString());
    Assert.assertEquals(appId.getVersion(), appDetails.get("appVersion").getAsString());

    // Get app info for the app with default versionId by versioned API
    JsonObject appDetailsDefault = getAppDetails(appId.getNamespace(), appId.getApplication(),
                                                 ApplicationId.DEFAULT_VERSION);
    Assert.assertEquals(GSON.toJson(configDefault), appDetailsDefault.get("configuration").getAsString());
    Assert.assertEquals(ApplicationId.DEFAULT_VERSION, appDetailsDefault.get("appVersion").getAsString());

    // Get app info for the app with versionId "version_2" by versioned API
    JsonObject appDetailsV2 = getAppDetails(appId.getNamespace(), appId.getApplication(), appIdV2.getVersion());
    Assert.assertEquals(GSON.toJson(configV2), appDetailsV2.get("configuration").getAsString());

    // Update app with default versionId by versioned API
    ConfigTestApp.ConfigClass configDefault2 = new ConfigTestApp.ConfigClass("mno", "pqr");
    AppRequest<ConfigTestApp.ConfigClass> requestDefault2 = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), configDefault2);
    Assert.assertEquals(200, deploy(appIdDefault.toEntityId(), requestDefault2).getStatusLine().getStatusCode());

    JsonObject appDetailsDefault2 = getAppDetails(appIdDefault.getNamespaceId(), appIdDefault.getId());
    Assert.assertEquals(GSON.toJson(configDefault2), appDetailsDefault2.get("configuration").getAsString());

    // Get updated app info for the app with default versionId by versioned API
    JsonObject appDetailsDefault2WithVersion = getAppDetails(appIdDefault.getNamespaceId(), appIdDefault.getId(),
                                                             ApplicationId.DEFAULT_VERSION);
    Assert.assertEquals(GSON.toJson(configDefault2), appDetailsDefault2WithVersion.get("configuration").getAsString());
    Assert.assertEquals(ApplicationId.DEFAULT_VERSION, appDetailsDefault.get("appVersion").getAsString());
    deleteApp(appId, 200);
    deleteApp(appIdDefault, 200);
    deleteApp(appIdV2, 200);
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
    Id.Namespace ns2 = Id.Namespace.from(TEST_NAMESPACE2);

    Id.Artifact ns2ArtifactId = Id.Artifact.from(ns2, "bloatedListAndGet", "1.0.0-SNAPSHOT");

    //deploy without name to testnamespace1
    HttpResponse response = deploy(BloatedWordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    //deploy with name to testnamespace2
    response = addAppArtifact(ns2ArtifactId, BloatedWordCountApp.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Id.Application appId = Id.Application.from(ns2, appName);
    response = deploy(appId, new AppRequest<Config>(ArtifactSummary.from(ns2ArtifactId.toArtifactId())));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertNotNull(response.getEntity());

    // deploy with name and version to testnamespace2
    ApplicationId app1 = new ApplicationId(TEST_NAMESPACE2, appName, VERSION1);
    response = deploy(app1, new AppRequest<Config>(ArtifactSummary.from(ns2ArtifactId.toArtifactId())));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertNotNull(response.getEntity());

    //verify testnamespace1 has 1 app
    List<JsonObject> apps = getAppList(TEST_NAMESPACE1);
    Assert.assertEquals(1, apps.size());

    //verify testnamespace2 has 2 app
    apps = getAppList(TEST_NAMESPACE2);
    Assert.assertEquals(2, apps.size());

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

    //get and verify app details in testnamespace2
    result = getAppDetails(TEST_NAMESPACE2, appName, VERSION1);
    Assert.assertEquals(appName, result.get("name").getAsString());

    //delete app in testnamespace1
    response = doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    //delete app in testnamespace2
    response = doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    deleteArtifact(ns2ArtifactId, 200);

    //verify testnamespace2 has 0 app
    apps = getAppList(TEST_NAMESPACE2);
    Assert.assertEquals(0, apps.size());
  }

  /**
   * Tests deleting applications with versioned and non-versioned API.
   */
  @Test
  public void testDelete() throws Exception {
    // Delete an non-existing app
    HttpResponse response = doDelete(getVersionedAPIPath("apps/XYZ", Constants.Gateway.API_VERSION_3_TOKEN,
                                                         TEST_NAMESPACE1));
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    // Start a fow for the App
    deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Id.Program program = Id.Program.from(TEST_NAMESPACE1, "WordCountApp", ProgramType.FLOW, "WordCountFlow");
    startProgram(program);
    waitState(program, "RUNNING");
    // Try to delete an App while its flow is running
    response = doDelete(getVersionedAPIPath("apps/WordCountApp", Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE1));
    Assert.assertEquals(409, response.getStatusLine().getStatusCode());
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
    Assert.assertEquals(409, response.getStatusLine().getStatusCode());
    Assert.assertEquals("'" + program.getNamespace() +
                          "' could not be deleted. Reason: The following programs are still running: "
                          + program.getApplicationId() + ": " + program.getId(), readResponse(response));

    stopProgram(program);
    waitState(program, "STOPPED");

    // Delete the app in the wrong namespace
    response = doDelete(getVersionedAPIPath("apps/WordCountApp", Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE2));
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    // Delete an non-existing app with version
    response = doDelete(getVersionedAPIPath("apps/XYZ/versions/" + VERSION1,
                                                         Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    // Deploy an app with version
    Id.Artifact wordCountArtifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "wordcountapp", VERSION1);
    addAppArtifact(wordCountArtifactId, WordCountApp.class);
    AppRequest<? extends Config> wordCountRequest = new AppRequest<>(
      new ArtifactSummary(wordCountArtifactId.getName(), wordCountArtifactId.getVersion().getVersion()));
    ApplicationId wordCountApp1 = NamespaceId.DEFAULT.app("WordCountApp", VERSION1);
    Assert.assertEquals(200, deploy(wordCountApp1, wordCountRequest).getStatusLine().getStatusCode());

    // Start a flow for the App
    ProgramId program1 = wordCountApp1.program(ProgramType.FLOW, "WordCountFlow");
    startProgram(program1, 200);
    waitState(program1, "RUNNING");
    // Try to delete an App while its flow is running
    response = doDelete(getVersionedAPIPath(
      String.format("apps/%s/versions/%s", wordCountApp1.getApplication(), wordCountApp1.getVersion()),
      Constants.Gateway.API_VERSION_3_TOKEN, wordCountApp1.getNamespace()));
    Assert.assertEquals(409, response.getStatusLine().getStatusCode());
    Assert.assertEquals("'" + program1.getParent() + "' could not be deleted. Reason: The following programs" +
                          " are still running: " + program1.getProgram(), readResponse(response));

    stopProgram(program1, null, 200, null);
    waitState(program1, "STOPPED");

    // Delete the app with version in the wrong namespace
    response = doDelete(getVersionedAPIPath(
      String.format("apps/%s/versions/%s", wordCountApp1.getApplication(), wordCountApp1.getVersion()),
      Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2));
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    //Delete the app with version after stopping the flow
    response = doDelete(getVersionedAPIPath(
      String.format("apps/%s/versions/%s", wordCountApp1.getApplication(), wordCountApp1.getVersion()),
      Constants.Gateway.API_VERSION_3_TOKEN, wordCountApp1.getNamespace()));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doDelete(getVersionedAPIPath(
      String.format("apps/%s/versions/%s", wordCountApp1.getApplication(), wordCountApp1.getVersion()),
      Constants.Gateway.API_VERSION_3_TOKEN, wordCountApp1.getNamespace()));
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

    List<ArtifactSummary> summaries = readResponse(response, new TypeToken<List<ArtifactSummary>>() {
    }.getType());
    Assert.assertFalse(summaries.isEmpty());

    // cleanup
    deleteNamespace(NamespaceId.DEFAULT.getNamespace());
  }

  private static class ExtraConfig extends Config {
    @SuppressWarnings("unused")
    private final int x = 5;
  }
}
