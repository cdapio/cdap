/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services.http.handlers;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.AppWithDataset;
import io.cdap.cdap.AppWithDatasetDuplicate;
import io.cdap.cdap.AppWithNoServices;
import io.cdap.cdap.AppWithSchedule;
import io.cdap.cdap.ConfigTestApp;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.gateway.handlers.AppLifecycleHttpHandler;
import io.cdap.cdap.internal.app.deploy.Specifications;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.BatchApplicationDetail;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.common.http.HttpResponse;
import org.jboss.resteasy.util.HttpResponseCodes;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.StreamSupport;

/**
 * Tests for {@link AppLifecycleHttpHandler}
 */
public class AppLifecycleHttpHandlerTest extends AppFabricTestBase {

  /**
   * Tests deploying an application in a non-existing non-default namespace.
   */
  @Test
  public void testDeployNonExistingNamespace() throws Exception {
    HttpResponse response = deploy(AllProgramsApp.class, 404, Constants.Gateway.API_VERSION_3_TOKEN, "random");
    NotFoundException nfe = new NamespaceNotFoundException(new NamespaceId("random"));
    Assert.assertEquals(nfe.getMessage(), response.getResponseBodyAsString());
  }

  /**
   * Tests deploying an application.
   */
  @Test
  public void testDeployValid() throws Exception {
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    HttpResponse response = doDelete(getVersionedAPIPath("apps/",
                                                         Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getResponseCode());
  }

  @Test
  public void testDeployWithExtraConfig() throws Exception {
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "extraConfig", "1.0.0-SNAPSHOT");
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "ExtraConfigApp");
    HttpResponse response = addAppArtifact(artifactId, AppWithNoServices.class);
    Assert.assertEquals(200, response.getResponseCode());
    response = deploy(appId, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId()), new ExtraConfig()));
    Assert.assertEquals(200, response.getResponseCode());
    deleteApp(appId, 200);
    deleteArtifact(artifactId, 200);
  }

  @Test
  public void testAppWithConfig() throws Exception {
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp");
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "appWithConfig", "1.0.0-SNAPSHOT");
    HttpResponse response = addAppArtifact(artifactId, ConfigTestApp.class);
    Assert.assertEquals(200, response.getResponseCode());

    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("abc", "def");
    response = deploy(appId, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId()), config));
    Assert.assertEquals(200, response.getResponseCode());
    JsonObject appDetails = getAppDetails(Id.Namespace.DEFAULT.getId(), "ConfigApp");
    Assert.assertEquals(GSON.toJson(config), appDetails.get("configuration").getAsString());

    deleteApp(appId, 200);
    deleteArtifact(artifactId, 200);
  }

  @Test
  public void testAppWithConfigurationString() throws Exception {
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp");
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "appWithConfig", "1.0.0-SNAPSHOT");
    HttpResponse response = addAppArtifact(artifactId, ConfigTestApp.class);
    Assert.assertEquals(200, response.getResponseCode());

    ConfigTestApp.ConfigClass configuration = new ConfigTestApp.ConfigClass("abc", "def");
    String configurationString = GSON.toJson(configuration);
    response = deploy(appId, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId()), null, null, null, null,
                                              configurationString));
    Assert.assertEquals(200, response.getResponseCode());
    JsonObject appDetails = getAppDetails(Id.Namespace.DEFAULT.getId(), "ConfigApp");
    Assert.assertEquals(GSON.toJson(configuration), appDetails.get("configuration").getAsString());

    deleteApp(appId, 200);
    deleteArtifact(artifactId, 200);
  }

  @Test
  public void testAppWithConfiguration() throws Exception {
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp");
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "appWithConfig", "1.0.0-SNAPSHOT");
    HttpResponse response = addAppArtifact(artifactId, ConfigTestApp.class);
    Assert.assertEquals(200, response.getResponseCode());

    ConfigTestApp.ConfigClass configuration = new ConfigTestApp.ConfigClass("abc", "def");
    response = deploy(appId, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId()), null, null, null, null,
                                              configuration));
    Assert.assertEquals(200, response.getResponseCode());
    JsonObject appDetails = getAppDetails(Id.Namespace.DEFAULT.getId(), "ConfigApp");
    Assert.assertEquals(GSON.toJson(configuration), appDetails.get("configuration").getAsString());

    deleteApp(appId, 200);
    deleteArtifact(artifactId, 200);
  }

  @Test
  public void testDeployUsingNonexistantArtifact404() throws Exception {
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "badapp");
    AppRequest<Config> appRequest =
      new AppRequest<>(new ArtifactSummary("something", "1.0.0"), null);
    HttpResponse response = deploy(appId, appRequest);
    Assert.assertEquals(404, response.getResponseCode());
  }

  @Test
  public void testDeployUsingArtifact() throws Exception {
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "configapp", "1.0.0");
    addAppArtifact(artifactId, ConfigTestApp.class);

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "cfgApp");
    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("abc", "def");
    AppRequest<ConfigTestApp.ConfigClass> request = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), config);
    Assert.assertEquals(200, deploy(appId, request).getResponseCode());

    JsonObject appDetails = getAppDetails(Id.Namespace.DEFAULT.getId(), appId.getId());
    Assert.assertEquals(GSON.toJson(config), appDetails.get("configuration").getAsString());

    Assert.assertEquals(200,
      doDelete(getVersionedAPIPath("apps/" + appId.getId(), appId.getNamespaceId())).getResponseCode());
  }

  @Test
  public void testOwnerUsingArtifact() throws Exception {
    ArtifactId artifactId = new ArtifactId(NamespaceId.DEFAULT.getNamespace(), "artifact", "1.0.0");
    addAppArtifact(Id.Artifact.fromEntityId(artifactId), AllProgramsApp.class);
    ApplicationId applicationId = new ApplicationId(NamespaceId.DEFAULT.getNamespace(), AllProgramsApp.NAME);
    // deploy an app with a owner
    String ownerPrincipal = "alice/somehost.net@somekdc.net";
    AppRequest<ConfigTestApp.ConfigClass> appRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()), null, ownerPrincipal);
    Assert.assertEquals(HttpResponseCodes.SC_OK, deploy(applicationId, appRequest).getResponseCode());

    // should be able to retrieve the owner information of the app
    JsonObject appDetails = getAppDetails(NamespaceId.DEFAULT.getNamespace(), applicationId.getApplication());
    Assert.assertEquals(ownerPrincipal, appDetails.get(Constants.Security.PRINCIPAL).getAsString());

    // the dataset created by the app should have the app owner too
    Assert.assertEquals(ownerPrincipal,
                        getDatasetMeta(applicationId.getNamespaceId().dataset(AllProgramsApp.DATASET_NAME))
                          .getOwnerPrincipal());

    // trying to deploy the same app with another owner should fail
    String bobPrincipal = "bob/somehost.net@somekdc.net";
    appRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()), null, bobPrincipal);
    Assert.assertEquals(HttpResponseCodes.SC_FORBIDDEN,
                        deploy(applicationId, appRequest).getResponseCode());

    // trying to deploy the same app with different version and another owner should fail too
    appRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()), null, bobPrincipal);
    Assert.assertEquals(HttpResponseCodes.SC_FORBIDDEN,
                        deploy(new ApplicationId(applicationId.getNamespace(), applicationId.getApplication(), "1.0"),
                               appRequest).getResponseCode());

    // trying to re-deploy the same app with same owner should pass
    appRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()), null, ownerPrincipal);
    Assert.assertEquals(HttpResponseCodes.SC_OK,
                        deploy(applicationId, appRequest).getResponseCode());

    // trying to re-deploy the same app with different version but same owner should pass
    appRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()), null, ownerPrincipal);
    Assert.assertEquals(HttpResponseCodes.SC_OK,
                        deploy(new ApplicationId(applicationId.getNamespace(), applicationId.getApplication(), "1.0"),
                               appRequest).getResponseCode());

    // clean up the app
    Assert.assertEquals(200,
                        doDelete(getVersionedAPIPath("apps/" + applicationId.getApplication(),
                                                     applicationId.getNamespace())).getResponseCode());

    // deletion of app should delete the dataset owner information as they themselves are not deleted
    Assert.assertEquals(ownerPrincipal,
                        getDatasetMeta(applicationId.getNamespaceId().dataset(AllProgramsApp.DATASET_NAME))
                          .getOwnerPrincipal());

    // cleanup
    deleteNamespace(NamespaceId.DEFAULT.getNamespace());
  }

  @Test
  public void testOwnerInHeaders() throws Exception {
    String ownerPrincipal = "bob/somehost.net@somekdc.net";
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN,
           NamespaceId.DEFAULT.getNamespace(), ownerPrincipal);

    ApplicationId applicationId = new ApplicationId(NamespaceId.DEFAULT.getNamespace(), AllProgramsApp.NAME);

    // should be able to retrieve the owner information of the app
    JsonObject appDetails = getAppDetails(NamespaceId.DEFAULT.getNamespace(), applicationId.getApplication());
    Assert.assertEquals(ownerPrincipal, appDetails.get(Constants.Security.PRINCIPAL).getAsString());

    // cleanup app
    Assert.assertEquals(200,
                        doDelete(getVersionedAPIPath("apps/" + applicationId.getApplication(),
                                                     applicationId.getNamespace())).getResponseCode());

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
    Assert.assertEquals(200, deploy(appId, request).getResponseCode());
    // Cannot update the app created by versioned API with versionId not ending with "-SNAPSHOT"
    Assert.assertEquals(409, deploy(appId, request).getResponseCode());
    Assert.assertEquals(404, getAppResponse(Id.Namespace.DEFAULT.getId(), appId.getApplication(),
                                            "non_existing_version").getResponseCode());
    Assert.assertEquals(404, getAppResponse(Id.Namespace.DEFAULT.getId(),
                                            appId.getApplication()).getResponseCode());

    // Deploy app with default versionId by non-versioned API
    Id.Application appIdDefault = Id.Application.from(Id.Namespace.DEFAULT, appId.getApplication());
    ConfigTestApp.ConfigClass configDefault = new ConfigTestApp.ConfigClass("uvw", "xyz");
    AppRequest<ConfigTestApp.ConfigClass> requestDefault = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), configDefault);
    Assert.assertEquals(200, deploy(appIdDefault, requestDefault).getResponseCode());

    // Deploy app with versionId "version_2" by versioned API
    ApplicationId appIdV2 = new ApplicationId(appId.getNamespace(), appId.getApplication(), "2.0.0");
    ConfigTestApp.ConfigClass configV2 = new ConfigTestApp.ConfigClass("ghi", "jkl");
    AppRequest<ConfigTestApp.ConfigClass> requestV2 = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), configV2);
    Assert.assertEquals(200, deploy(appIdV2, requestV2).getResponseCode());

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
    Assert.assertEquals(200, deploy(appIdDefault.toEntityId(), requestDefault2).getResponseCode());

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
    deploy(String.class, 400, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
  }

  /**
   * Tests deploying an application with dataset same name as existing dataset but a different type
   */
  @Test
  public void testDeployFailure() throws Exception {
    deploy(AppWithDataset.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    deploy(AppWithDatasetDuplicate.class, 400, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
  }

  @Test
  public void testListNonExistentNamespace() throws Exception {
    HttpResponse response = doGet(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN,
                                                      NONEXISTENT_NAMESPACE));
    Assert.assertEquals(404, response.getResponseCode());
  }

  @Test
  public void testListAndGet() throws Exception {
    //deploy without name to testnamespace1
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);

    //deploy with name to testnamespace2
    String ns2AppName = AllProgramsApp.NAME + "2";
    Id.Namespace ns2 = Id.Namespace.from(TEST_NAMESPACE2);
    Id.Artifact ns2ArtifactId = Id.Artifact.from(ns2, AllProgramsApp.class.getSimpleName(), "1.0.0-SNAPSHOT");

    HttpResponse response = addAppArtifact(ns2ArtifactId, AllProgramsApp.class);
    Assert.assertEquals(200, response.getResponseCode());
    Id.Application appId = Id.Application.from(ns2, ns2AppName);
    response = deploy(appId, new AppRequest<>(ArtifactSummary.from(ns2ArtifactId.toArtifactId())));
    Assert.assertEquals(200, response.getResponseCode());

    // deploy with name and version to testnamespace2
    ApplicationId app1 = new ApplicationId(TEST_NAMESPACE2, ns2AppName, VERSION1);
    response = deploy(app1, new AppRequest<>(ArtifactSummary.from(ns2ArtifactId.toArtifactId())));
    Assert.assertEquals(200, response.getResponseCode());

    //verify testnamespace1 has 1 app
    List<JsonObject> apps = getAppList(TEST_NAMESPACE1);
    Assert.assertEquals(1, apps.size());

    //verify testnamespace2 has 2 app
    apps = getAppList(TEST_NAMESPACE2);
    Assert.assertEquals(2, apps.size());

    //get and verify app details in testnamespace1
    JsonObject result = getAppDetails(TEST_NAMESPACE1, AllProgramsApp.NAME);
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());

    Assert.assertEquals(AllProgramsApp.NAME, result.get("name").getAsString());
    Assert.assertEquals(AllProgramsApp.DESC, result.get("description").getAsString());

    // Validate the datasets
    JsonArray datasets = result.get("datasets").getAsJsonArray();
    Assert.assertEquals(spec.getDatasets().size(), datasets.size());
    Assert.assertTrue(
      StreamSupport.stream(datasets.spliterator(), false)
        .map(JsonObject.class::cast)
        .map(obj -> obj.get("name").getAsString())
        .allMatch(dataset -> spec.getDatasets().containsKey(dataset))
    );

    // Validate the programs
    JsonArray programs = result.get("programs").getAsJsonArray();
    int totalPrograms = Arrays.stream(io.cdap.cdap.api.app.ProgramType.values())
      .mapToInt(type -> spec.getProgramsByType(type).size())
      .reduce(0, (l, r) -> l + r);
    Assert.assertEquals(totalPrograms, programs.size());

    Assert.assertTrue(
      StreamSupport.stream(programs.spliterator(), false)
        .map(JsonObject.class::cast)
        .allMatch(obj -> {
          String type = obj.get("type").getAsString().toUpperCase();
          io.cdap.cdap.api.app.ProgramType programType = io.cdap.cdap.api.app.ProgramType.valueOf(type);
          return spec.getProgramsByType(programType).contains(obj.get("name").getAsString());
        })
    );

    //get and verify app details in testnamespace2
    List<BatchApplicationDetail> appDetails = getAppDetails(TEST_NAMESPACE2,
                                                            Arrays.asList(ImmutablePair.of(ns2AppName, null),
                                                                          ImmutablePair.of(ns2AppName, VERSION1)));
    Assert.assertEquals(2, appDetails.size());
    Assert.assertTrue(appDetails.stream().allMatch(d -> d.getStatusCode() == 200));

    ApplicationDetail appDetail = appDetails.get(0).getDetail();
    Assert.assertNotNull(appDetail);
    Assert.assertEquals(ns2AppName, appDetail.getName());
    Assert.assertEquals(ApplicationId.DEFAULT_VERSION, appDetail.getAppVersion());

    appDetail = appDetails.get(1).getDetail();
    Assert.assertNotNull(appDetail);
    Assert.assertEquals(ns2AppName, appDetail.getName());
    Assert.assertEquals(VERSION1, appDetail.getAppVersion());

    //delete app in testnamespace1
    response = doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getResponseCode());

    //delete app in testnamespace2
    response = doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2));
    Assert.assertEquals(200, response.getResponseCode());
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
    Assert.assertEquals(404, response.getResponseCode());

    // Start a service from the App
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Id.Program program = Id.Program.from(TEST_NAMESPACE1, AllProgramsApp.NAME,
                                         ProgramType.SERVICE, AllProgramsApp.NoOpService.NAME);
    startProgram(program);
    waitState(program, "RUNNING");
    // Try to delete an App while its service is running
    response = doDelete(getVersionedAPIPath("apps/" + AllProgramsApp.NAME, Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE1));
    Assert.assertEquals(409, response.getResponseCode());
    Assert.assertEquals("'" + program.getApplication() +
                          "' could not be deleted. Reason: The following programs are still running: "
                          + program.getId(), response.getResponseBodyAsString());

    stopProgram(program);
    waitState(program, "STOPPED");

    startProgram(program);
    waitState(program, "RUNNING");
    // Try to delete all Apps while service is running
    response = doDelete(getVersionedAPIPath("apps", Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE1));
    Assert.assertEquals(409, response.getResponseCode());
    Assert.assertEquals("'" + program.getNamespace() +
                          "' could not be deleted. Reason: The following programs are still running: "
                          + program.getApplicationId() + ": " + program.getId(),
                        response.getResponseBodyAsString());

    stopProgram(program);
    waitState(program, "STOPPED");

    // Delete the app in the wrong namespace
    response = doDelete(getVersionedAPIPath("apps/" + AllProgramsApp.NAME, Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE2));
    Assert.assertEquals(404, response.getResponseCode());

    // Delete an non-existing app with version
    response = doDelete(getVersionedAPIPath("apps/XYZ/versions/" + VERSION1,
                                                         Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(404, response.getResponseCode());

    // Deploy an app with version
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, AllProgramsApp.class.getSimpleName(), VERSION1);
    addAppArtifact(artifactId, AllProgramsApp.class);
    AppRequest<? extends Config> appRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()));
    ApplicationId appId = NamespaceId.DEFAULT.app(AllProgramsApp.NAME, VERSION1);
    Assert.assertEquals(200, deploy(appId, appRequest).getResponseCode());

    // Start a service for the App
    ProgramId program1 = appId.program(ProgramType.SERVICE, AllProgramsApp.NoOpService.NAME);
    startProgram(program1, 200);
    waitState(program1, "RUNNING");
    // Try to delete an App while its service is running
    response = doDelete(getVersionedAPIPath(
      String.format("apps/%s/versions/%s", appId.getApplication(), appId.getVersion()),
      Constants.Gateway.API_VERSION_3_TOKEN, appId.getNamespace()));
    Assert.assertEquals(409, response.getResponseCode());
    Assert.assertEquals("'" + program1.getParent() + "' could not be deleted. Reason: The following programs" +
                          " are still running: " + program1.getProgram(), response.getResponseBodyAsString());

    stopProgram(program1, null, 200, null);
    waitState(program1, "STOPPED");

    // Delete the app with version in the wrong namespace
    response = doDelete(getVersionedAPIPath(
      String.format("apps/%s/versions/%s", appId.getApplication(), appId.getVersion()),
      Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2));
    Assert.assertEquals(404, response.getResponseCode());

    //Delete the app with version after stopping the service
    response = doDelete(getVersionedAPIPath(
      String.format("apps/%s/versions/%s", appId.getApplication(), appId.getVersion()),
      Constants.Gateway.API_VERSION_3_TOKEN, appId.getNamespace()));
    Assert.assertEquals(200, response.getResponseCode());
    response = doDelete(getVersionedAPIPath(
      String.format("apps/%s/versions/%s", appId.getApplication(), appId.getVersion()),
      Constants.Gateway.API_VERSION_3_TOKEN, appId.getNamespace()));
    Assert.assertEquals(404, response.getResponseCode());

    //Delete the App after stopping the service
    response = doDelete(getVersionedAPIPath("apps/" + AllProgramsApp.NAME, Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getResponseCode());
    response = doDelete(getVersionedAPIPath("apps/" + AllProgramsApp.NAME, Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE1));
    Assert.assertEquals(404, response.getResponseCode());

    // deleting the app should not delete the artifact
    response = doGet(getVersionedAPIPath("artifacts/" + artifactId.getName(), Constants.Gateway.API_VERSION_3_TOKEN,
                                         TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getResponseCode());

    List<ArtifactSummary> summaries = readResponse(response, new TypeToken<List<ArtifactSummary>>() {
    }.getType());
    Assert.assertFalse(summaries.isEmpty());

    // cleanup
    deleteNamespace(NamespaceId.DEFAULT.getNamespace());
  }

  @Test
  public void testDeployAppWithDisabledProfileInSchedule() throws Exception {
    // put my profile and disable it
    ProfileId profileId = new NamespaceId(TEST_NAMESPACE1).profile("MyProfile");
    Profile profile = new Profile("MyProfile", Profile.NATIVE.getLabel(), Profile.NATIVE.getDescription(),
                                   Profile.NATIVE.getScope(), Profile.NATIVE.getProvisioner());
    putProfile(profileId, profile, 200);
    disableProfile(profileId, 200);

    // deploy an app with schedule with some disabled profile in the schedule property
    AppWithSchedule.AppConfig config =
      new AppWithSchedule.AppConfig(true, true, true,
                                    ImmutableMap.of(SystemArguments.PROFILE_NAME, "USER:MyProfile"));

    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.fromEntityId(TEST_NAMESPACE_META1.getNamespaceId()),
      AppWithSchedule.NAME, VERSION1);
    addAppArtifact(artifactId, AppWithSchedule.class);
    AppRequest<? extends Config> request = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), config, null, null, true);

    // deploy should fail with a 409
    ApplicationId defaultAppId = TEST_NAMESPACE_META1.getNamespaceId().app(AppWithSchedule.NAME);
    Assert.assertEquals(409, deploy(defaultAppId, request).getResponseCode());

    // enable
    enableProfile(profileId, 200);
    Assert.assertEquals(200, deploy(defaultAppId, request).getResponseCode());

    // disable again so that we can delete it at namespace deletion
    disableProfile(profileId, 200);

    // clean up
    deleteApp(defaultAppId, 200);
    deleteArtifact(artifactId, 200);
  }

  private static class ExtraConfig extends Config {
    @SuppressWarnings("unused")
    private final int x = 5;
  }
}
