/*
 * Copyright © 2023 Cask Data, Inc.
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
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.config.PreferencesService;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.gateway.handlers.SourceControlManagementHttpHandler;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.services.SourceControlManagementService;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.metadata.MetadataSubscriberService;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.AuthConfig;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.PatConfig;
import io.cdap.cdap.proto.sourcecontrol.Provider;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfigRequest;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfigValidationException;
import io.cdap.cdap.proto.sourcecontrol.RepositoryMeta;
import io.cdap.cdap.proto.sourcecontrol.RepositoryValidationFailure;
import io.cdap.cdap.proto.sourcecontrol.SetRepositoryResponse;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import io.cdap.cdap.scheduler.ProgramScheduleService;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.sourcecontrol.AuthenticationConfigException;
import io.cdap.cdap.sourcecontrol.NoChangesToPullException;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.SourceControlException;
import io.cdap.cdap.sourcecontrol.operationrunner.PushAppResponse;
import io.cdap.cdap.sourcecontrol.operationrunner.RepositoryApp;
import io.cdap.cdap.sourcecontrol.operationrunner.RepositoryAppsResponse;
import io.cdap.cdap.sourcecontrol.operationrunner.SourceControlOperationRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.common.http.HttpResponse;
import java.util.Arrays;
import java.util.Map;
import javax.annotation.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for {@link SourceControlManagementHttpHandler}
 */
public class SourceControlManagementHttpHandlerTests extends AppFabricTestBase {
  private static CConfiguration cConf;
  private static SourceControlManagementService sourceControlService;
  private static final String FEATURE_FLAG_PREFIX = "feature.";
  private static final String NAME = "testNamespace";
  private static final String LINK = "example.com";
  private static final String DEFAULT_BRANCH = "develop";
  private static final String PASSWORD_NAME = "test_password_name";
  private static final String USERNAME = "test_user";
  private static final String TEST_FIELD = "test";
  private static final String REPO_FIELD = "repository";
  private static final Gson GSON = new Gson();
  private static final AuthConfig AUTH_CONFIG = new AuthConfig(
      AuthType.PAT, new PatConfig(PASSWORD_NAME, USERNAME));

  @BeforeClass
  public static void beforeClass() throws Throwable {
    cConf = createBasicCConf();
    initializeAndStartServices(cConf);
    sourceControlService = getInjector().getInstance(SourceControlManagementService.class);
  }

  @Before
  public void before() {
    Mockito.reset(sourceControlService);
    setScmFeatureFlag(true);
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
      public SourceControlManagementService provideSourceControlManagementService(
        CConfiguration cConf,
        SecureStore secureStore,
        TransactionRunner transactionRunner,
        AccessEnforcer accessEnforcer,
        AuthenticationContext authenticationContext,
        SourceControlOperationRunner sourceControlRunner,
        ApplicationLifecycleService applicationLifecycleService,
        PreferencesService preferencesService,
        ProgramScheduleService programScheduleService,
        Store store) {
        return Mockito.spy(new SourceControlManagementService(cConf, secureStore, transactionRunner,
                                                              accessEnforcer, authenticationContext,
                                                              sourceControlRunner, applicationLifecycleService,
                                                              preferencesService, programScheduleService,
                                                              store));
      }
    });
  }

  private static void setScmFeatureFlag(boolean flag) {
    cConf.setBoolean(FEATURE_FLAG_PREFIX + Feature.SOURCE_CONTROL_MANAGEMENT_GIT.getFeatureFlagString(), flag);
  }

  private void assertResponseCode(int expected, HttpResponse response) {
    Assert.assertEquals(expected, response.getResponseCode());
  }

  private RepositoryMeta readGetRepositoryMeta(HttpResponse response) {
    return readResponse(response, RepositoryMeta.class);
  }

  @Test
  public void testNamespaceRepository() throws Exception {
    // verify the repository does not exist at the beginning
    HttpResponse response = getRepository(NAME);
    assertResponseCode(404, response);

    // Set repository config
    RepositoryConfig namespaceRepo = new RepositoryConfig.Builder()
        .setProvider(Provider.GITHUB)
        .setLink(LINK)
        .setDefaultBranch(DEFAULT_BRANCH)
        .setAuth(AUTH_CONFIG)
        .build();

    // Assert the namespace does not exist
    response = setRepository(NAME, createRepoRequestString(namespaceRepo));
    assertResponseCode(404, response);

    // Create the NS
    assertResponseCode(200, createNamespace(NAME));

    // Set and verify the repository
    response = setRepository(NAME, createRepoRequestString(namespaceRepo));
    assertResponseCode(200, response);

    response = getRepository(NAME);
    RepositoryMeta repository = readGetRepositoryMeta(response);
    Assert.assertEquals(namespaceRepo, repository.getConfig());
    Assert.assertNotEquals(0, repository.getUpdatedTimeMillis());

    RepositoryConfig newRepoConfig = new RepositoryConfig.Builder(namespaceRepo)
        .setAuth(new AuthConfig(AuthType.PAT, new PatConfig("a new password name", null)))
        .setLink("a new link")
        .build();

    response = setRepository(NAME, createRepoRequestString(newRepoConfig));
    assertResponseCode(200, response);
    response = getRepository(NAME);
    repository = readGetRepositoryMeta(response);

    // verify that the repo config has been updated
    Assert.assertEquals(newRepoConfig, repository.getConfig());
    Assert.assertNotEquals(0, repository.getUpdatedTimeMillis());

    // Delete the repository
    assertResponseCode(200, deleteRepository(NAME));
    assertResponseCode(404, getRepository(NAME));

    // cleanup
    response = deleteNamespace(NAME);
    assertResponseCode(200, response);
  }

  @Test
  public void testInvalidRepoConfig() throws Exception {
    // Create the NS
    assertResponseCode(200, createNamespace(NAME));

    // verify the repository does not exist at the beginning
    HttpResponse response = getRepository(NAME);
    assertResponseCode(404, response);

    // Set the invalid repository that's missing the whole configuration object
    assertResponseCode(400, setRepository(NAME, GSON.toJson(ImmutableMap.of(TEST_FIELD, "false"))));
    assertResponseCode(404, getRepository(NAME));

    // Set the invalid repository that's missing provider
    String configMissingProvider = buildRepoRequestString(
        null, LINK, DEFAULT_BRANCH,
        new AuthConfig(AuthType.PAT, new PatConfig(PASSWORD_NAME, USERNAME)),
        null);
    assertResponseCode(400, setRepository(NAME, configMissingProvider));
    assertResponseCode(404, getRepository(NAME));

    // Set the invalid repository that's missing link
    String configMissingLink = buildRepoRequestString(
        Provider.GITHUB, null, DEFAULT_BRANCH,
        new AuthConfig(AuthType.PAT, new PatConfig(PASSWORD_NAME, USERNAME)),
        null);
    assertResponseCode(400, setRepository(NAME, configMissingLink));
    assertResponseCode(404, getRepository(NAME));

    // Set the invalid repository that's missing auth token name
    String configMissingPasswordName = buildRepoRequestString(
        Provider.GITHUB, LINK, "",
        new AuthConfig(AuthType.PAT, new PatConfig("", USERNAME)),
        null);
    assertResponseCode(400, setRepository(NAME, configMissingPasswordName));
    assertResponseCode(404, getRepository(NAME));

    // cleanup
    response = deleteNamespace(NAME);
    assertResponseCode(200, response);
  }

  @Test
  public void testFeatureFlagDisabledEndpoint() throws Exception {
    setScmFeatureFlag(false);

    // Set repository config
    RepositoryConfig namespaceRepo = new RepositoryConfig.Builder()
        .setProvider(Provider.GITHUB)
        .setLink(LINK)
        .setDefaultBranch(DEFAULT_BRANCH)
        .setAuth(AUTH_CONFIG)
        .build();

    // Assert the namespace does not exist
    assertResponseCode(403, setRepository(NAME, createRepoRequestString(namespaceRepo)));
    assertResponseCode(403, getRepository(NAME));
    assertResponseCode(403, deleteRepository(NAME));
  }

  @Test
  public void testValidateRepoConfigSuccess() throws Exception {
    RepositoryConfig config = new RepositoryConfig.Builder()
        .setProvider(Provider.GITHUB)
        .setLink(LINK)
        .setDefaultBranch(DEFAULT_BRANCH)
        .setAuth(AUTH_CONFIG)
        .build();

    Mockito.doNothing()
      .when(sourceControlService)
      .validateRepository(Mockito.any(), Mockito.any());

    HttpResponse response = setRepository(NAME, createRepoValidateString(config));
    assertResponseCode(200, response);
  }

  @Test
  public void testValidateRepoConfigFails() throws Exception {
    RepositoryConfig config = new RepositoryConfig.Builder()
        .setProvider(Provider.GITHUB)
        .setLink(LINK)
        .setDefaultBranch(DEFAULT_BRANCH)
        .setAuth(AUTH_CONFIG)
        .build();
    RepositoryConfigValidationException ex =
      new RepositoryConfigValidationException(Arrays.asList(new RepositoryValidationFailure("fake error 1"),
                                                            new RepositoryValidationFailure("another fake error")));

    Mockito.doThrow(ex).when(sourceControlService).validateRepository(Mockito.any(), Mockito.any());
    SetRepositoryResponse expectedResponse = new SetRepositoryResponse(ex);

    HttpResponse response = setRepository(NAME, createRepoValidateString(config));
    SetRepositoryResponse actualResponse = readResponse(response, SetRepositoryResponse.class);
    assertResponseCode(400, response);
    Assert.assertEquals(actualResponse, expectedResponse);
  }

  @Test
  public void testPushAppSucceeds() throws Exception {
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp", "version1");

    // Push one application to linked repository
    String commitMessage = "push one app";
    PushAppResponse expectedAppResponse = new PushAppResponse(appId1.getId(), appId1.getVersion(),
                                                              appId1.getId() + " hash");
    Mockito.doReturn(expectedAppResponse).when(sourceControlService)
      .pushApp(Mockito.any(), Mockito.eq(commitMessage));
    HttpResponse response = pushApplication(NamespaceId.DEFAULT.appReference(appId1.getId()), commitMessage);

    // Assert the app is pushed
    assertResponseCode(200, response);
    PushAppResponse result = readResponse(response, PushAppResponse.class);
    Assert.assertEquals(result, expectedAppResponse);
  }

  @Test
  public void testPushAppInvalidRequest() throws Exception {
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp", "version1");
    // Push empty commit message
    String commitMessage = "";
    HttpResponse response = pushApplication(NamespaceId.DEFAULT.appReference(appId1.getId()), commitMessage);

    // Assert the response
    assertResponseCode(400, response);
    Assert.assertEquals(response.getResponseBodyAsString(),
        "Please specify commit message in the request body.");
  }

  @Test
  public void testPushAppNotFound() throws Exception {
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp", "version1");

    // Push one application to linked repository
    String commitMessage = "push one app";
    Mockito.doThrow(new NotFoundException("apps not found")).when(sourceControlService)
      .pushApp(Mockito.any(), Mockito.eq(commitMessage));
    HttpResponse response = pushApplication(NamespaceId.DEFAULT.appReference(appId1.getId()), commitMessage);

    // Assert the app is not found
    assertResponseCode(404, response);
    Assert.assertEquals(response.getResponseBodyAsString(), "apps not found");
  }

  @Test
  public void testPushAppNoChange() throws Exception {
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp", "version1");

    // Push one application to linked repository
    String commitMessage = "push one app";
    Mockito.doThrow(new NoChangesToPushException("No changes for apps to push")).when(sourceControlService)
      .pushApp(Mockito.any(), Mockito.eq(commitMessage));
    HttpResponse response = pushApplication(NamespaceId.DEFAULT.appReference(appId1.getId()), commitMessage);

    // Assert the error that app has no changes to push
    assertResponseCode(200, response);
    Assert.assertTrue(response.getResponseBodyAsString().contains("No changes for apps to push"));
  }

  @Test
  public void testPushAppSourceControlException() throws Exception {
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp", "version1");

    // Push one application to linked repository
    String commitMessage = "push one app";
    Mockito.doThrow(new SourceControlException("Failed to push app")).when(sourceControlService)
      .pushApp(Mockito.any(), Mockito.eq(commitMessage));
    HttpResponse response = pushApplication(NamespaceId.DEFAULT.appReference(appId1.getId()), commitMessage);

    // Assert the SourceControlException
    assertResponseCode(500, response);
    Assert.assertTrue(response.getResponseBodyAsString().contains("Failed to push app"));
  }

  @Test
  public void testPushAppInvalidAuthenticationConfig() throws Exception {
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp", "version1");

    // Push one application to linked repository
    String commitMessage = "push one app";
    Mockito.doThrow(new AuthenticationConfigException("Repository config not valid")).when(sourceControlService)
      .pushApp(Mockito.any(), Mockito.eq(commitMessage));
    HttpResponse response = pushApplication(NamespaceId.DEFAULT.appReference(appId1.getId()), commitMessage);

    // Assert the AuthenticationConfigException
    assertResponseCode(500, response);
    Assert.assertTrue(response.getResponseBodyAsString().contains("Repository config not valid"));
  }

  @Test
  public void testPullAppSucceeds() throws Exception {
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp", "version1");

    // Pull one application from linked repository
    SourceControlMeta meta = new SourceControlMeta("fileHash");
    ApplicationRecord expectedAppResponse = new ApplicationRecord(new ArtifactSummary("name", "version"),
                                                                  appId1.getId(), appId1.getVersion(), "",
                                                                  null, null, meta);
    Mockito.doReturn(expectedAppResponse).when(sourceControlService).pullAndDeploy(Mockito.any());
    HttpResponse response = pullApplication(NamespaceId.DEFAULT.appReference(appId1.getId()));

    // Assert one app is pulled and deployed
    assertResponseCode(200, response);
    ApplicationRecord result = readResponse(response, ApplicationRecord.class);
    Assert.assertEquals(result, expectedAppResponse);
  }

  @Test
  public void testPullAppNotFound() throws Exception {
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp", "version1");

    // Pull one application from linked repository
    Mockito.doThrow(new NotFoundException("app ConfigApp not found"))
      .when(sourceControlService).pullAndDeploy(Mockito.any());
    HttpResponse response = pullApplication(NamespaceId.DEFAULT.appReference(appId1.getId()));

    // Assert the app is not found
    assertResponseCode(404, response);
    Assert.assertEquals(response.getResponseBodyAsString(), "app ConfigApp not found");
  }

  @Test
  public void testPullAppNoChange() throws Exception {
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp", "version1");

    // Push two applications to linked repository
    Mockito.doThrow(new NoChangesToPullException("No changes for app to pull"))
      .when(sourceControlService).pullAndDeploy(Mockito.any());
    HttpResponse response = pullApplication(NamespaceId.DEFAULT.appReference(appId1.getId()));

    // Assert the app is not changed
    assertResponseCode(200, response);
    Assert.assertTrue(response.getResponseBodyAsString().contains("No changes for app to pull"));
  }

  @Test
  public void testPullAppInvalidAuthenticationConfig() throws Exception {
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp", "version1");

    // Pull one application from repository
    Mockito.doThrow(new AuthenticationConfigException("Repo configuration is invalid"))
      .when(sourceControlService).pullAndDeploy(Mockito.any());
    HttpResponse response = pullApplication(NamespaceId.DEFAULT.appReference(appId1.getId()));

    // Assert the AuthenticationConfigException
    assertResponseCode(500, response);
    Assert.assertTrue(response.getResponseBodyAsString().contains("Repo configuration is invalid"));
  }

  @Test
  public void testPullAppSourceControlException() throws Exception {
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp", "version1");

    // Pull one application from repository
    Mockito.doThrow(new SourceControlException("Failed to pull application"))
      .when(sourceControlService).pullAndDeploy(Mockito.any());
    HttpResponse response = pullApplication(NamespaceId.DEFAULT.appReference(appId1.getId()));

    // Assert the pull failure
    assertResponseCode(500, response);
    Assert.assertTrue(response.getResponseBodyAsString().contains("Failed to pull application"));
  }

  @Test
  public void testListAppsSucceed() throws Exception {
    RepositoryApp app1 = new RepositoryApp("app1", "hash1");
    RepositoryApp app2 = new RepositoryApp("app2", "hash2");
    RepositoryAppsResponse expectedListResult = new RepositoryAppsResponse(Arrays.asList(app1, app2));
    Mockito.doReturn(expectedListResult).when(sourceControlService)
      .listApps(Mockito.any());

    HttpResponse response = listApplicationsFromRepository(Id.Namespace.DEFAULT.getId());
    assertResponseCode(200, response);
    RepositoryAppsResponse result = readResponse(response, RepositoryAppsResponse.class);

    Assert.assertEquals(result.getApps(), expectedListResult.getApps());
  }

  @Test
  public void testListAppsFailed() throws Exception {
    Mockito.doThrow(SourceControlException.class).when(sourceControlService)
      .listApps(Mockito.any());

    HttpResponse response = listApplicationsFromRepository(Id.Namespace.DEFAULT.getId());
    assertResponseCode(500, response);
  }

  @Test
  public void testListAppsFailedAuth() throws Exception {
    Mockito.doThrow(AuthenticationConfigException.class).when(sourceControlService)
      .listApps(Mockito.any());

    HttpResponse response = listApplicationsFromRepository(Id.Namespace.DEFAULT.getId());
    assertResponseCode(500, response);
  }

  @Test
  public void testListAppsNotFound() throws Exception {
    Mockito.doThrow(new NotFoundException("apps not found")).when(sourceControlService)
      .listApps(Mockito.any());
    HttpResponse response = listApplicationsFromRepository(Id.Namespace.DEFAULT.getId());
    assertResponseCode(404, response);
  }

  private String buildRepoRequestString(Provider provider, String link, String defaultBranch,
                                        AuthConfig authConfig, @Nullable String pathPrefix) {
    Map<String, Object> patJsonMap = ImmutableMap.of(
        "passwordName", authConfig.getPatConfig().getPasswordName(),
        "username", authConfig.getPatConfig().getUsername());
    Map<String, Object> authJsonMap = ImmutableMap.of(
        "type", authConfig.getType().toString(),
        "patConfig", patJsonMap);
    Map<String, Object> repoString = ImmutableMap.of(
        "provider", provider == null ? "" : provider.toString(),
        "link", link == null ? "" : link,
        "defaultBranch", defaultBranch,
        "pathPrefix", pathPrefix == null ? "" : pathPrefix,
        "auth", authJsonMap);
    return GSON.toJson(ImmutableMap.of(TEST_FIELD, "false", REPO_FIELD, repoString));
  }

  private String createRepoRequestString(RepositoryConfig namespaceRepo) {
    return GSON.toJson(new RepositoryConfigRequest(namespaceRepo));
  }

  private String createRepoValidateString(RepositoryConfig namespaceRepo) {
    return GSON.toJson(new RepositoryConfigRequest(namespaceRepo, true));
  }
}
