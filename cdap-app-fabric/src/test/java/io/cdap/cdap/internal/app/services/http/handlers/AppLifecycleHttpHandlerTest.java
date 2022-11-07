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
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.AppWithDataset;
import io.cdap.cdap.AppWithDatasetDuplicate;
import io.cdap.cdap.AppWithNoServices;
import io.cdap.cdap.AppWithSchedule;
import io.cdap.cdap.ConfigTestApp;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.metrics.MetricsSystemClient;
import io.cdap.cdap.app.deploy.ManagerFactory;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.config.PreferencesService;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.data2.registry.UsageRegistry;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.gateway.handlers.AppLifecycleHttpHandler;
import io.cdap.cdap.internal.app.deploy.Specifications;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.capability.CapabilityReader;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.metadata.MetadataSubscriberService;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.DatasetDetail;
import io.cdap.cdap.proto.ProgramRecord;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.scheduler.Scheduler;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.common.http.HttpResponse;
import org.jboss.resteasy.util.HttpResponseCodes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Tests for {@link AppLifecycleHttpHandler}
 */
public class AppLifecycleHttpHandlerTest extends AppFabricTestBase {
  private static CConfiguration cConf;
  private static final String FEATURE_FLAG_PREFIX = "feature.";

  @BeforeClass
  public static void beforeClass() throws Throwable {
    cConf = createBasicCConf();
    initializeAndStartServices(cConf);
  }

  private void setLCMFlag(boolean lcmFlag) {
    cConf.setBoolean(FEATURE_FLAG_PREFIX + Feature.LIFECYCLE_MANAGEMENT_EDIT.getFeatureFlagString(), lcmFlag);
  }

  @Before
  public void resetMock() {
    Mockito.reset(getInjector().getInstance(ApplicationLifecycleService.class));
  }

  protected static void initializeAndStartServices(CConfiguration cConf) throws Exception {
    initializeAndStartServices(cConf, new AbstractModule() {
      @Override
      protected void configure() {
        bind(UGIProvider.class).to(CurrentUGIProvider.class);
        bind(MetadataSubscriberService.class).in(Scopes.SINGLETON);
      }

      @Provides
      @Singleton
      public ApplicationLifecycleService createLifeCycleService(CConfiguration cConf,
          Store store, Scheduler scheduler, UsageRegistry usageRegistry,
          PreferencesService preferencesService, MetricsSystemClient metricsSystemClient,
          OwnerAdmin ownerAdmin, ArtifactRepository artifactRepository,
          ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms> managerFactory,
          MetadataServiceClient metadataServiceClient,
          AccessEnforcer accessEnforcer, AuthenticationContext authenticationContext,
          MessagingService messagingService, Impersonator impersonator,
          CapabilityReader capabilityReader) {

        return Mockito.spy(new ApplicationLifecycleService(cConf, store, scheduler,
            usageRegistry, preferencesService, metricsSystemClient, ownerAdmin, artifactRepository,
            managerFactory, metadataServiceClient, accessEnforcer, authenticationContext,
            messagingService, impersonator, capabilityReader, new NoOpMetricsCollectionService()));
      }
    });
  }

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
    ApplicationDetail appDetails = getAppDetails(Id.Namespace.DEFAULT.getId(), "ConfigApp");
    Assert.assertEquals(GSON.toJson(config), appDetails.getConfiguration());

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
    ApplicationDetail appDetails = getAppDetails(Id.Namespace.DEFAULT.getId(), "ConfigApp");
    Assert.assertEquals(GSON.toJson(configuration), appDetails.getConfiguration());

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
    ApplicationDetail appDetails = getAppDetails(Id.Namespace.DEFAULT.getId(), "ConfigApp");
    Assert.assertEquals(GSON.toJson(configuration), appDetails.getConfiguration());

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

    ApplicationDetail appDetails = getAppDetails(Id.Namespace.DEFAULT.getId(), appId.getId());
    Assert.assertEquals(GSON.toJson(config), appDetails.getConfiguration());

    Assert.assertEquals(200,
      doDelete(getVersionedAPIPath("apps/" + appId.getId(), appId.getNamespaceId())).getResponseCode());
  }

  @Test
  public void testOwnerUsingArtifact() throws Exception {
    ArtifactId artifactId = new ArtifactId(NamespaceId.DEFAULT.getNamespace(), "artifact", "1.0.0");
    addAppArtifact(Id.Artifact.fromEntityId(artifactId), AllProgramsApp.class);
    ApplicationId applicationId = new ApplicationId(Id.Namespace.DEFAULT.getId(), AllProgramsApp.NAME);
    // deploy an app with a owner
    String ownerPrincipal = "alice/somehost.net@somekdc.net";
    AppRequest<ConfigTestApp.ConfigClass> appRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()), null, ownerPrincipal);
    HttpResponse deploy = deploy(applicationId, appRequest);
    Assert.assertEquals(HttpResponseCodes.SC_OK, deploy.getResponseCode());

    // should be able to retrieve the owner information of the app
    ApplicationDetail appDetails = getAppDetails(NamespaceId.DEFAULT.getNamespace(), applicationId.getApplication());
    Assert.assertEquals(ownerPrincipal, appDetails.getOwnerPrincipal());

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
    ApplicationDetail appDetails = getAppDetails(NamespaceId.DEFAULT.getNamespace(), applicationId.getApplication());
    Assert.assertEquals(ownerPrincipal, appDetails.getOwnerPrincipal());

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
    Set<String> versions = ImmutableSet.of("-SNAPSHOT", "2.0.0", "1.0.0");
    ApplicationId appId = new ApplicationId(Id.Namespace.DEFAULT.getId(), "cfgAppWithVersion", "1.0.0");
    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("abc", "def");
    AppRequest<ConfigTestApp.ConfigClass> request = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), config);
    Assert.assertEquals(200, deploy(appId, request).getResponseCode());
    // Can update the app created by versioned API with versionId not ending with "-SNAPSHOT"
    Assert.assertEquals(200, deploy(appId, request).getResponseCode());
    Assert.assertEquals(404, getAppResponse(Id.Namespace.DEFAULT.getId(), appId.getApplication(),
                                            "non_existing_version").getResponseCode());
    Assert.assertEquals(200, getAppResponse(Id.Namespace.DEFAULT.getId(),
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


    Assert.assertEquals(versions, getAppVersions(appId.getNamespace(), appId.getApplication()));

    List<JsonObject> appList = getAppList(appId.getNamespace());
    Set<String> receivedVersions = new HashSet<>();
    for (JsonObject appRecord : appList) {
      receivedVersions.add(appRecord.getAsJsonPrimitive("version").getAsString());
    }
    Assert.assertEquals(versions, receivedVersions);

    ApplicationDetail appDetails = getAppDetails(appId.getNamespace(), appId.getApplication(), appId.getVersion());
    Assert.assertEquals(appId.getVersion(), appDetails.getAppVersion());
    Assert.assertEquals(GSON.toJson(config), appDetails.getConfiguration());

    // Get app info for the app with default versionId by versioned API
    ApplicationDetail appDetailsDefault = getAppDetails(appId.getNamespace(), appId.getApplication(),
                                                        ApplicationId.DEFAULT_VERSION);
    // Introduced in LCM when trying to retrieve -SNAPSHOT, return latest
    Assert.assertEquals(appIdV2.getVersion(), appDetailsDefault.getAppVersion());
    Assert.assertEquals(GSON.toJson(configV2), appDetailsDefault.getConfiguration());

    // Get app info for the app with versionId "version_2" by versioned API
    ApplicationDetail appDetailsV2 = getAppDetails(appId.getNamespace(), appId.getApplication(), appIdV2.getVersion());
    Assert.assertEquals(GSON.toJson(configV2), appDetailsV2.getConfiguration());
    Assert.assertEquals(appIdV2.getVersion(), appDetailsV2.getAppVersion());

    // Update app with default versionId by versioned API
    ConfigTestApp.ConfigClass configDefault2 = new ConfigTestApp.ConfigClass("mno", "pqr");
    AppRequest<ConfigTestApp.ConfigClass> requestDefault2 = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), configDefault2);
    Assert.assertEquals(200, deploy(appIdDefault.toEntityId(), requestDefault2).getResponseCode());

    ApplicationDetail appDetailsDefault2 = getAppDetails(appIdDefault.getNamespaceId(), appIdDefault.getId());
    Assert.assertEquals(GSON.toJson(configDefault2), appDetailsDefault2.getConfiguration());

    // Get updated app info for the app with default versionId by versioned API
    ApplicationDetail appDetailsDefault2WithVersion = getAppDetails(appIdDefault.getNamespaceId(),
                                                                    appIdDefault.getId(),
                                                                    ApplicationId.DEFAULT_VERSION);
    Assert.assertEquals(GSON.toJson(configDefault2), appDetailsDefault2WithVersion.getConfiguration());

    Id.Application appIdDelete = Id.Application.from(appId.getNamespace(), appId.getApplication());
    deleteApp(appIdDelete, 200);
  }

  @Test
  public void testDeployVersionedAndNonVersionedAppLCMFlagEnabled() throws Exception {
    setLCMFlag(true);
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "configapp", "1.0.0");
    addAppArtifact(artifactId, ConfigTestApp.class);
    Set<String> versions = new HashSet<>();
    ApplicationId appId = new ApplicationId(Id.Namespace.DEFAULT.getId(), "cfgAppWithVersion", "1.0.0");
    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("abc", "def");
    AppRequest<ConfigTestApp.ConfigClass> request = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), config);
    Assert.assertEquals(200, deploy(appId, request).getResponseCode());
    ApplicationDetail appDetails = getAppDetails(Id.Namespace.DEFAULT.getId(), "cfgAppWithVersion");
    String appIdVersion = appDetails.getAppVersion();
    versions.add(appIdVersion);
    // Can update the app created by versioned API with versionId not ending with "-SNAPSHOT"
    Assert.assertEquals(200, deploy(appId, request).getResponseCode());
    appDetails = getAppDetails(appId.getNamespace(), appId.getApplication());
    versions.add(appDetails.getAppVersion());
    Assert.assertEquals(404, getAppResponse(Id.Namespace.DEFAULT.getId(), appId.getApplication(),
                                            "non_existing_version").getResponseCode());
    Assert.assertEquals(200, getAppResponse(Id.Namespace.DEFAULT.getId(),
                                            appId.getApplication()).getResponseCode());

    // Deploy app with default versionId by non-versioned API
    Id.Application appIdDefault = Id.Application.from(Id.Namespace.DEFAULT, appId.getApplication());
    ConfigTestApp.ConfigClass configDefault = new ConfigTestApp.ConfigClass("uvw", "xyz");
    AppRequest<ConfigTestApp.ConfigClass> requestDefault = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), configDefault);
    Assert.assertEquals(200, deploy(appIdDefault, requestDefault).getResponseCode());
    appDetails = getAppDetails(Id.Namespace.DEFAULT.getId(), appId.getApplication());

    String appIdDefaultVersion = appDetails.getAppVersion();
    versions.add(appIdDefaultVersion);

    // Deploy app with versionId "version_2" by versioned API
    ApplicationId appIdV2 = new ApplicationId(appId.getNamespace(), appId.getApplication(), "2.0.0");
    ConfigTestApp.ConfigClass configV2 = new ConfigTestApp.ConfigClass("ghi", "jkl");
    AppRequest<ConfigTestApp.ConfigClass> requestV2 = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), configV2);
    Assert.assertEquals(200, deploy(appIdV2, requestV2).getResponseCode());
    appDetails = getAppDetails(Id.Namespace.DEFAULT.getId(), "cfgAppWithVersion");
    String appIdV2Version = appDetails.getAppVersion();
    versions.add(appIdV2Version);

    Assert.assertEquals(versions, getAppVersions(appId.getNamespace(), appId.getApplication()));

    List<JsonObject> appList = getAppList(appId.getNamespace());
    Set<String> receivedVersions = new HashSet<>();
    for (JsonObject appRecord : appList) {
      receivedVersions.add(appRecord.getAsJsonPrimitive("version").getAsString());
    }
    Assert.assertEquals(versions, receivedVersions);

    appDetails = getAppDetails(appId.getNamespace(), appId.getApplication(), appIdVersion);
    Assert.assertEquals(appIdVersion, appDetails.getAppVersion());
    // when LCM is enabled a new app version is generated else -SNAPSHOT is overwritten
    Assert.assertEquals(GSON.toJson(config), appDetails.getConfiguration());

    // Get app info for the app with default versionId by versioned API
    ApplicationDetail appDetailsDefault = getAppDetails(appId.getNamespace(), appId.getApplication(),
                                                        appIdDefaultVersion);
    Assert.assertEquals(appIdDefaultVersion, appDetailsDefault.getAppVersion());
    // when LCM is enabled a new app version is generated else -SNAPSHOT is overwritten
    Assert.assertEquals(GSON.toJson(configDefault), appDetailsDefault.getConfiguration());

    // Get app info for the app with versionId "version_2" by versioned API
    ApplicationDetail appDetailsV2 = getAppDetails(appId.getNamespace(), appId.getApplication(), appIdV2Version);
    Assert.assertEquals(GSON.toJson(configV2), appDetailsV2.getConfiguration());
    Assert.assertEquals(appIdV2Version, appDetailsV2.getAppVersion());

    // Update app with default versionId by versioned API
    ConfigTestApp.ConfigClass configDefault2 = new ConfigTestApp.ConfigClass("mno", "pqr");
    AppRequest<ConfigTestApp.ConfigClass> requestDefault2 = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), configDefault2);
    Assert.assertEquals(200, deploy(appIdDefault.toEntityId(), requestDefault2).getResponseCode());

    ApplicationDetail appDetailsDefault2 = getAppDetails(appIdDefault.getNamespaceId(), appIdDefault.getId());
    Assert.assertEquals(GSON.toJson(configDefault2), appDetailsDefault2.getConfiguration());

    // Get updated app info for the app with default versionId by versioned API
    ApplicationDetail appDetailsDefault2WithVersion = getAppDetails(appIdDefault.getNamespaceId(),
                                                                    appIdDefault.getId());
    Assert.assertEquals(GSON.toJson(configDefault2), appDetailsDefault2WithVersion.getConfiguration());

    Id.Application appIdDelete = Id.Application.from(appId.getNamespace(), appId.getApplication());
    deleteApp(appIdDelete, 200);
    setLCMFlag(false);
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
    HttpResponse response = doDelete(getVersionedAPIPath("apps/",
                                                         Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getResponseCode());
    List<JsonObject> apps = getAppList(TEST_NAMESPACE1);
    Assert.assertEquals(0, apps.size());
  }

  @Test
  public void testListNonExistentNamespace() throws Exception {
    HttpResponse response = doGet(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN,
                                                      NONEXISTENT_NAMESPACE));
    Assert.assertEquals(404, response.getResponseCode());
  }

  @Test
  public void testListAndGetForPaginatedAPI() throws Exception {
    for (int i = 0; i < 10; i++) {
      //deploy with name to testnamespace1
      String ns1AppName = AllProgramsApp.NAME + i;
      Id.Namespace ns1 = Id.Namespace.from(TEST_NAMESPACE1);
      Id.Artifact ns1ArtifactId = Id.Artifact.from(ns1, AllProgramsApp.class.getSimpleName(),
          "1.0.0-SNAPSHOT");

      HttpResponse response = addAppArtifact(ns1ArtifactId, AllProgramsApp.class);
      Assert.assertEquals(200, response.getResponseCode());
      Id.Application appId = Id.Application.from(ns1, ns1AppName);
      response = deploy(appId,
          new AppRequest<>(ArtifactSummary.from(ns1ArtifactId.toArtifactId())));
      Assert.assertEquals(200, response.getResponseCode());
    }

    int count = 0;
    String token = null;
    boolean isLastPage = false;
    boolean emptyListReceived = false;

    while (!isLastPage) {
      JsonObject result = getAppListForPaginatedApi(TEST_NAMESPACE1, 3, token, "");
      int currentResultSize = result.get("applications").getAsJsonArray().size();
      count += currentResultSize;
      emptyListReceived = (currentResultSize == 0);
      token = result.get("nextPageToken") == null ? null : result.get("nextPageToken").getAsString();
      isLastPage = (token == null);
    }
    Assert.assertEquals(10, count);
    Assert.assertFalse(emptyListReceived);

    //delete app in testnamespace1
    HttpResponse response = doDelete(getVersionedAPIPath("apps/",
                                     Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getResponseCode());
    List<JsonObject> apps = getAppList(TEST_NAMESPACE1);
    Assert.assertEquals(0, apps.size());
  }

  @Test
  public void testListAndGetForPaginatedAPIWithEmptyLastPage() throws Exception {
    for (int i = 0; i < 9; i++) {
      //deploy with name to testnamespace1
      String ns1AppName = AllProgramsApp.NAME + i;
      Id.Namespace ns1 = Id.Namespace.from(TEST_NAMESPACE1);
      Id.Artifact ns1ArtifactId = Id.Artifact.from(ns1, AllProgramsApp.class.getSimpleName(),
          "1.0.0-SNAPSHOT");

      HttpResponse response = addAppArtifact(ns1ArtifactId, AllProgramsApp.class);
      Assert.assertEquals(200, response.getResponseCode());
      Id.Application appId = Id.Application.from(ns1, ns1AppName);
      response = deploy(appId,
          new AppRequest<>(ArtifactSummary.from(ns1ArtifactId.toArtifactId())));
      Assert.assertEquals(200, response.getResponseCode());
    }

    int count = 0;
    String token = null;
    boolean isLastPage = false;
    boolean emptyListReceived = false;

    while (!isLastPage) {
      JsonObject result = getAppListForPaginatedApi(TEST_NAMESPACE1, 3, token, "");
      int currentResultSize = result.get("applications").getAsJsonArray().size();
      count += currentResultSize;
      emptyListReceived = (currentResultSize == 0);
      token = result.get("nextPageToken") == null ? null : result.get("nextPageToken").getAsString();
      isLastPage = (token == null);
    }
    Assert.assertEquals(9, count);
    Assert.assertTrue(emptyListReceived);

    //delete app in testnamespace1
    HttpResponse response = doDelete(getVersionedAPIPath("apps/",
        Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getResponseCode());
    List<JsonObject> apps = getAppList(TEST_NAMESPACE1);
    Assert.assertEquals(0, apps.size());
  }

  @Test
  public void testListAndGetForPaginatedAPIWithFiltering() throws Exception {
    List<Integer> filteredIndices = new ArrayList<>(Arrays.asList(1, 2, 4, 5, 7, 9));
    for (int i = 0; i < 10; i++) {
      //deploy with name to testnamespace1
      String ns1AppName = AllProgramsApp.NAME + i;
      if (filteredIndices.contains(i)) {
        ns1AppName = AllProgramsApp.NAME + "filter" + i;
      }

      Id.Namespace ns1 = Id.Namespace.from(TEST_NAMESPACE1);
      Id.Artifact ns1ArtifactId = Id.Artifact.from(ns1, AllProgramsApp.class.getSimpleName(),
          "1.0.0-SNAPSHOT");

      HttpResponse response = addAppArtifact(ns1ArtifactId, AllProgramsApp.class);
      Assert.assertEquals(200, response.getResponseCode());
      Id.Application appId = Id.Application.from(ns1, ns1AppName);
      response = deploy(appId,
          new AppRequest<>(ArtifactSummary.from(ns1ArtifactId.toArtifactId())));
      Assert.assertEquals(200, response.getResponseCode());
    }

    int count = 0;
    String token = null;
    boolean isLastPage = false;
    boolean emptyListReceived = false;

    while (!isLastPage) {
      JsonObject result = getAppListForPaginatedApi(TEST_NAMESPACE1, 3, token, "filter");
      int currentResultSize = result.get("applications").getAsJsonArray().size();
      count += currentResultSize;
      emptyListReceived = (currentResultSize == 0);
      token = result.get("nextPageToken") == null ? null : result.get("nextPageToken").getAsString();
      isLastPage = (token == null);
    }
    Assert.assertEquals(6, count);
    Assert.assertTrue(emptyListReceived);

    //delete app in testnamespace1
    HttpResponse response = doDelete(getVersionedAPIPath("apps/",
        Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getResponseCode());
    List<JsonObject> apps = getAppList(TEST_NAMESPACE1);
    Assert.assertEquals(0, apps.size());
  }

  @Test
  public void testListAndGetForPaginatedAPIWithNameFilterType() throws Exception {
    deleteApp(Id.Application.from(TEST_NAMESPACE1, AllProgramsApp.NAME), 200);
    for (int i = 0; i < 2; i++) {
      //deploy with name to testnamespace1
      String ns1AppName = AllProgramsApp.NAME + String.join("", Collections.nCopies(i, "1"));
      Id.Namespace ns1 = Id.Namespace.from(TEST_NAMESPACE1);
      Id.Artifact ns1ArtifactId = Id.Artifact.from(ns1, AllProgramsApp.class.getSimpleName(),
                                                   "1.0.0-SNAPSHOT");

      HttpResponse response = addAppArtifact(ns1ArtifactId, AllProgramsApp.class);
      Assert.assertEquals(200, response.getResponseCode());
      Id.Application appId = Id.Application.from(ns1, ns1AppName);
      response = deploy(appId,
                        new AppRequest<>(ArtifactSummary.from(ns1ArtifactId.toArtifactId())));
      Assert.assertEquals(200, response.getResponseCode());
    }
    String token = null;
    JsonObject result = getAppListForPaginatedApi(TEST_NAMESPACE1, 3, token,
                                                  AllProgramsApp.NAME.toUpperCase(Locale.ROOT),
                                                  "EQUALS", null, null);
    int currentResultSize = result.get("applications").getAsJsonArray().size();
    Assert.assertEquals(0, currentResultSize);

    JsonObject result1 = getAppListForPaginatedApi(TEST_NAMESPACE1, 3, token,
                                                   AllProgramsApp.NAME.toUpperCase(Locale.ROOT),
                                                  "EQUALS_IGNORE_CASE", null, null);
    int currentResultSize1 = result1.get("applications").getAsJsonArray().size();
    Assert.assertEquals(1, currentResultSize1);

    JsonObject result2 = getAppListForPaginatedApi(TEST_NAMESPACE1, 3, token, AllProgramsApp.NAME, null, null, null);
    int currentResultSize2 = result2.get("applications").getAsJsonArray().size();
    Assert.assertEquals(2, currentResultSize2);

    //delete app in testnamespace1
    HttpResponse response = doDelete(getVersionedAPIPath("apps/",
                                                         Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getResponseCode());
    List<JsonObject> apps = getAppList(TEST_NAMESPACE1);
    Assert.assertEquals(0, apps.size());
  }

  @Test
  public void testListAndGetForPaginatedAPIWithLatestOnlyLCMFlagEnabled() throws Exception {
    setLCMFlag(true);
    // deploy 2 versions of the same app
    for (int i = 0; i < 2; i++) {
      Id.Namespace ns1 = Id.Namespace.from(TEST_NAMESPACE1);
      Id.Artifact ns1ArtifactId = Id.Artifact.from(ns1, AllProgramsApp.class.getSimpleName(),
                                                   "1.0.0-SNAPSHOT");

      HttpResponse response = addAppArtifact(ns1ArtifactId, AllProgramsApp.class);
      Assert.assertEquals(200, response.getResponseCode());
      ApplicationId appId = new ApplicationId(ns1.getId(), AllProgramsApp.NAME, "v" + i);
      response = deploy(appId,
                        new AppRequest<>(ArtifactSummary.from(ns1ArtifactId.toArtifactId())));
      Assert.assertEquals(200, response.getResponseCode());
    }
    // deploy 3 other different apps
    for (int i = 0; i < 3; i++) {
      //deploy with name to testnamespace1
      String ns1AppName = AllProgramsApp.NAME + i;
      Id.Namespace ns1 = Id.Namespace.from(TEST_NAMESPACE1);
      Id.Artifact ns1ArtifactId = Id.Artifact.from(ns1, AllProgramsApp.class.getSimpleName(),
                                                   "1.0.0-SNAPSHOT");

      HttpResponse response = addAppArtifact(ns1ArtifactId, AllProgramsApp.class);
      Assert.assertEquals(200, response.getResponseCode());
      Id.Application appId = Id.Application.from(ns1, ns1AppName);
      response = deploy(appId,
                        new AppRequest<>(ArtifactSummary.from(ns1ArtifactId.toArtifactId())));
      Assert.assertEquals(200, response.getResponseCode());
    }
    int count = 0;
    String token = null;
    boolean isLastPage = false;
    int currentResultSize = 0;
    while (!isLastPage) {
      JsonObject result = getAppListForPaginatedApi(TEST_NAMESPACE1, 3, token, AllProgramsApp.NAME, null, true, null);
      currentResultSize = result.get("applications").getAsJsonArray().size();
      count += currentResultSize;
      token = result.get("nextPageToken") == null ? null : result.get("nextPageToken").getAsString();
      isLastPage = (token == null);
    }
    Assert.assertEquals(4, count);
    Assert.assertEquals(1, currentResultSize);

    count = 0;
    token = null;
    isLastPage = false;
    currentResultSize = 0;
    while (!isLastPage) {
      JsonObject result = getAppListForPaginatedApi(TEST_NAMESPACE1, 3, token, AllProgramsApp.NAME, null, false, null);
      currentResultSize = result.get("applications").getAsJsonArray().size();
      count += currentResultSize;
      token = result.get("nextPageToken") == null ? null : result.get("nextPageToken").getAsString();
      isLastPage = (token == null);
    }
    Assert.assertEquals(5, count);
    Assert.assertEquals(2, currentResultSize);

    //delete app in testnamespace1
    HttpResponse response = doDelete(getVersionedAPIPath("apps/",
                                                         Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getResponseCode());
    List<JsonObject> apps = getAppList(TEST_NAMESPACE1);
    Assert.assertEquals(0, apps.size());
    setLCMFlag(false);
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

    // deploy the same app again to testnamespace2. This should create a new version if LCM is enabled
    // else it overrides -SNAPSHOT
    ApplicationId app1 = new ApplicationId(TEST_NAMESPACE2, ns2AppName);
    response = deploy(app1, new AppRequest<>(ArtifactSummary.from(ns2ArtifactId.toArtifactId())));
    Assert.assertEquals(200, response.getResponseCode());

    //verify testnamespace1 has 1 app
    List<JsonObject> apps = getAppList(TEST_NAMESPACE1);
    Assert.assertEquals(1, apps.size());

    //verify testnamespace2 has 2 app
    apps = getAppList(TEST_NAMESPACE2);
    Assert.assertEquals(1, apps.size());


    //get and verify app details in testnamespace1
    ApplicationDetail applicationDetail = getAppDetails(TEST_NAMESPACE1, AllProgramsApp.NAME);
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());

    Assert.assertEquals(AllProgramsApp.NAME, applicationDetail.getName());
    Assert.assertEquals(AllProgramsApp.DESC, applicationDetail.getDescription());

    // Validate the datasets
    List<DatasetDetail> datasetDetails = applicationDetail.getDatasets();
    Assert.assertEquals(spec.getDatasets().size(), datasetDetails.size());
    Assert.assertTrue(datasetDetails.stream()
                        .allMatch(dataset -> spec.getDatasets().containsKey(dataset.getName())));

    // Validate the programs
    List<ProgramRecord> programRecords = applicationDetail.getPrograms();
    int totalPrograms = Arrays.stream(io.cdap.cdap.api.app.ProgramType.values())
      .mapToInt(type -> spec.getProgramsByType(type).size())
      .reduce(0, Integer::sum);
    Assert.assertEquals(totalPrograms, programRecords.size());

    Assert.assertTrue(programRecords.stream().allMatch(
      programRecord -> {
        String type = programRecord.getType().toString().toUpperCase();
        io.cdap.cdap.api.app.ProgramType programType = io.cdap.cdap.api.app.ProgramType.valueOf(type);
        return spec.getProgramsByType(programType).contains(programRecord.getName());
      }
    ));

    //get and verify app details in testnamespace2. We expected two versions of the same app.
    apps = getAppList(TEST_NAMESPACE2);

    //Assert.assertTrue(ns2Apps.containsKey(ns2AppName));
    Assert.assertEquals(1, apps.size());
    Assert.assertEquals(ns2AppName, apps.get(0).get("name").getAsString());

    //delete app in testnamespace1
    response = doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getResponseCode());
    apps = getAppList(TEST_NAMESPACE1);
    Assert.assertTrue(apps.isEmpty());

    //delete app in testnamespace2
    response = doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2));
    Assert.assertEquals(200, response.getResponseCode());
    deleteArtifact(ns2ArtifactId, 200);

    //verify testnamespace2 has 0 app
    apps = getAppList(TEST_NAMESPACE2);
    Assert.assertTrue(apps.isEmpty());
  }

  @Test
  public void testListAndGetLCMFlagEnabled() throws Exception {
    setLCMFlag(true);
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

    // deploy the same app again to testnamespace2. This should create a new version if LCM is enabled
    // else it overrides -SNAPSHOT
    ApplicationId app1 = new ApplicationId(TEST_NAMESPACE2, ns2AppName);
    response = deploy(app1, new AppRequest<>(ArtifactSummary.from(ns2ArtifactId.toArtifactId())));
    Assert.assertEquals(200, response.getResponseCode());

    //verify testnamespace1 has 1 app
    List<JsonObject> apps = getAppList(TEST_NAMESPACE1);
    Assert.assertEquals(1, apps.size());

    //verify testnamespace2 has 2 app
    apps = getAppList(TEST_NAMESPACE2);
    Assert.assertEquals(2, apps.size());


    //get and verify app details in testnamespace1
    ApplicationDetail applicationDetail = getAppDetails(TEST_NAMESPACE1, AllProgramsApp.NAME);
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());

    Assert.assertEquals(AllProgramsApp.NAME, applicationDetail.getName());
    Assert.assertEquals(AllProgramsApp.DESC, applicationDetail.getDescription());

    // Validate the datasets
    List<DatasetDetail> datasetDetails = applicationDetail.getDatasets();
    Assert.assertEquals(spec.getDatasets().size(), datasetDetails.size());
    Assert.assertTrue(datasetDetails.stream()
                        .allMatch(dataset -> spec.getDatasets().containsKey(dataset.getName())));

    // Validate the programs
    List<ProgramRecord> programRecords = applicationDetail.getPrograms();
    int totalPrograms = Arrays.stream(io.cdap.cdap.api.app.ProgramType.values())
      .mapToInt(type -> spec.getProgramsByType(type).size())
      .reduce(0, Integer::sum);
    Assert.assertEquals(totalPrograms, programRecords.size());

    Assert.assertTrue(programRecords.stream().allMatch(
      programRecord -> {
        String type = programRecord.getType().toString().toUpperCase();
        io.cdap.cdap.api.app.ProgramType programType = io.cdap.cdap.api.app.ProgramType.valueOf(type);
        return spec.getProgramsByType(programType).contains(programRecord.getName());
      }
    ));

    //get and verify app details in testnamespace2. We expected two versions of the same app.
    apps = getAppList(TEST_NAMESPACE2);

    Map<String, List<ApplicationRecord>> ns2Apps = apps.stream()
      .map(jobj -> GSON.fromJson(jobj, ApplicationRecord.class))
      .collect(Collectors.groupingBy(ApplicationRecord::getName));

    Assert.assertTrue(ns2Apps.containsKey(ns2AppName));
    Assert.assertEquals(2, ns2Apps.get(ns2AppName).size());

    //delete app in testnamespace1
    response = doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getResponseCode());
    apps = getAppList(TEST_NAMESPACE1);
    Assert.assertTrue(apps.isEmpty());

    //delete app in testnamespace2
    response = doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2));
    Assert.assertEquals(200, response.getResponseCode());
    deleteArtifact(ns2ArtifactId, 200);

    //verify testnamespace2 has 0 app
    apps = getAppList(TEST_NAMESPACE2);
    Assert.assertTrue(apps.isEmpty());
    setLCMFlag(false);
  }

  @Test
  public void testListAndGetWithScanApplicationsException() throws Exception {
    String exceptionMessage = "sample_exception";
    Mockito.doThrow(new RuntimeException(exceptionMessage))
        .when(getInjector().getInstance(ApplicationLifecycleService.class))
        .scanApplications(Mockito.any(), Mockito.any());

    //deploy without name to testnamespace1
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);

    //verify getApps fails with error code 500
    HttpResponse response = getAppListResponseWhenFailingWithException(TEST_NAMESPACE1);
    Assert.assertEquals(500, response.getResponseCode());
    Assert.assertEquals(exceptionMessage, response.getResponseBodyAsString());
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

    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    ApplicationDetail appDetails1 = getAppDetails(TEST_NAMESPACE1, AllProgramsApp.NAME);
    ApplicationId appv1 = new ApplicationId(TEST_NAMESPACE1, AllProgramsApp.NAME,
                                                    appDetails1.getAppVersion());

    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    ApplicationDetail appDetails2 = getAppDetails(TEST_NAMESPACE1, AllProgramsApp.NAME);
    ApplicationId appv2 = new ApplicationId(TEST_NAMESPACE1, AllProgramsApp.NAME,
                                                    appDetails2.getAppVersion());
    deleteApp(appv1, 200);

    // Start a service from the App
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    ApplicationId applicationId = new ApplicationId(TEST_NAMESPACE1, AllProgramsApp.NAME);
    Id.Program program = Id.Program.from(TEST_NAMESPACE1, AllProgramsApp.NAME,
                                         ProgramType.SERVICE, AllProgramsApp.NoOpService.NAME);
    startProgram(program);
    waitState(program, "RUNNING");
    // Try to delete an App while its service is running
    response = doDelete(getVersionedAPIPath("apps/" + AllProgramsApp.NAME, Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE1));
    Assert.assertEquals(409, response.getResponseCode());
    Assert.assertEquals("'" + applicationId +
                          "' could not be deleted. Reason: The app has programs that are still running.",
                        response.getResponseBodyAsString());

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
    ApplicationDetail appDetails = getAppDetails(appId.getNamespace(), appId.getApplication());
    appId = new ApplicationId(appId.getNamespace(), appId.getApplication(), appDetails.getAppVersion());

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

  /**
   * Tests deleting applications with versioned and non-versioned API when LCM flag is enabled.
   */
  @Test
  public void testDeleteLCMFlagEnabled() throws Exception {
    setLCMFlag(true);
    // Delete an non-existing app
    HttpResponse response = doDelete(getVersionedAPIPath("apps/XYZ", Constants.Gateway.API_VERSION_3_TOKEN,
                                                         TEST_NAMESPACE1));
    Assert.assertEquals(404, response.getResponseCode());

    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    ApplicationDetail appDetails1 = getAppDetails(TEST_NAMESPACE1, AllProgramsApp.NAME);

    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    deleteApp(Id.Application.from(TEST_NAMESPACE1, AllProgramsApp.NAME), 200);

    // Start a service from the App
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    ApplicationId applicationId = new ApplicationId(TEST_NAMESPACE1, AllProgramsApp.NAME);
    Id.Program program = Id.Program.from(TEST_NAMESPACE1, AllProgramsApp.NAME,
                                         ProgramType.SERVICE, AllProgramsApp.NoOpService.NAME);
    startProgram(program);
    waitState(program, "RUNNING");
    // Try to delete an App while its service is running
    response = doDelete(getVersionedAPIPath("apps/" + AllProgramsApp.NAME, Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE1));
    Assert.assertEquals(409, response.getResponseCode());
    Assert.assertEquals("'" + applicationId +
                          "' could not be deleted. Reason: The app has programs that are still running.",
                        response.getResponseBodyAsString());

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
    response = doDelete(getVersionedAPIPath("apps/XYZ", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(404, response.getResponseCode());

    // Deploy an app with version
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, AllProgramsApp.class.getSimpleName(), VERSION1);
    addAppArtifact(artifactId, AllProgramsApp.class);
    AppRequest<? extends Config> appRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()));
    ApplicationId appId = NamespaceId.DEFAULT.app(AllProgramsApp.NAME, VERSION1);
    Assert.assertEquals(200, deploy(appId, appRequest).getResponseCode());
    ApplicationDetail appDetails = getAppDetails(appId.getNamespace(), appId.getApplication());
    appId = new ApplicationId(appId.getNamespace(), appId.getApplication(), appDetails.getAppVersion());

    // Start a service for the App
    ProgramId program1 = appId.program(ProgramType.SERVICE, AllProgramsApp.NoOpService.NAME);
    startProgram(program1, 200);
    waitState(program1, "RUNNING");
    // Try to delete an App while its service is running
    response = doDelete(getVersionedAPIPath(
      String.format("apps/%s", appId.getApplication()),
      Constants.Gateway.API_VERSION_3_TOKEN, appId.getNamespace()));
    Assert.assertEquals(409, response.getResponseCode());
    ProgramId programIdDefault = new ApplicationId(appId.getNamespace(), appId.getApplication())
      .program(ProgramType.SERVICE, AllProgramsApp.NoOpService.NAME);
    Assert.assertEquals("'" + programIdDefault.getParent() + "' could not be deleted. Reason: The app " +
                          "has programs that are still running.", response.getResponseBodyAsString());

    stopProgram(program1, null, 200, null);
    waitState(program1, "STOPPED");

    // Delete the app with version in the wrong namespace
    response = doDelete(getVersionedAPIPath(
      String.format("apps/%s", appId.getApplication()),
      Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2));
    Assert.assertEquals(404, response.getResponseCode());

    //Delete the app with version after stopping the service
    response = doDelete(getVersionedAPIPath(
      String.format("apps/%s", appId.getApplication()),
      Constants.Gateway.API_VERSION_3_TOKEN, appId.getNamespace()));
    Assert.assertEquals(200, response.getResponseCode());
    response = doDelete(getVersionedAPIPath(
      String.format("apps/%s", appId.getApplication()),
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
    setLCMFlag(false);
  }

  /**
   * Tests deleting with versioned API when LCM feature flag is enabled.
   */
  @Test
  public void testDeleteVersionedAppLCMEnabled() throws Exception {
    setLCMFlag(true);
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    ApplicationDetail appDetails = getAppDetails(TEST_NAMESPACE1, AllProgramsApp.NAME);

    HttpResponse response = doDelete(getVersionedAPIPath(
      String.format("apps/%s/versions/%s",  AllProgramsApp.NAME, appDetails.getAppVersion()),
      Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(403, response.getResponseCode());
    Assert.assertEquals("Forbidden", response.getResponseMessage());
    Assert.assertEquals("Deletion of specific app version is not allowed.", response.getResponseBodyAsString());
    Id.Application appIdDelete = Id.Application.from(TEST_NAMESPACE1, AllProgramsApp.NAME);
    deleteApp(appIdDelete, 200);
    setLCMFlag(false);
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
    ApplicationDetail appDetails = getAppDetails(defaultAppId.getNamespace(), defaultAppId.getApplication());
    defaultAppId =  new ApplicationId(defaultAppId.getNamespace(), defaultAppId.getApplication(),
                                      appDetails.getAppVersion());
    deleteApp(defaultAppId, 200);
    deleteArtifact(artifactId, 200);
  }

  @After
  public void cleanup() throws Exception {
    setLCMFlag(false);
  }

  private static class ExtraConfig extends Config {
    @SuppressWarnings("unused")
    private final int x = 5;
  }

  protected HttpResponse getAppListResponseWhenFailingWithException(String namespace) throws Exception {
    return doGet(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, namespace));
  }
}
