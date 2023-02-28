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
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.gateway.handlers.SourceControlManagementHttpHandler;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.services.SourceControlManagementService;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.metadata.MetadataSubscriberService;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.AuthConfig;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.Provider;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfigRequest;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfigValidationException;
import io.cdap.cdap.proto.sourcecontrol.RepositoryMeta;
import io.cdap.cdap.proto.sourcecontrol.RepositoryValidationFailure;
import io.cdap.cdap.proto.sourcecontrol.SetRepositoryResponse;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.operationrunner.PushAppResponse;
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
  private static final String TOKEN_NAME = "test_token_name";
  private static final String USERNAME = "test_user";
  private static final String TEST_FIELD = "test";
  private static final String REPO_FIELD = "repository";
  private static final Gson GSON = new Gson();

  @BeforeClass
  public static void beforeClass() throws Throwable {
    cConf = createBasicCConf();
    initializeAndStartServices(cConf);
    sourceControlService = getInjector().getInstance(SourceControlManagementService.class);
  }

  @Before
  public void before() {
    Mockito.reset(sourceControlService);
    setSCMFeatureFlag(true);
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
        Store store) {
        return Mockito.spy(new SourceControlManagementService(cConf, secureStore, transactionRunner,
                                                              accessEnforcer, authenticationContext,
                                                              sourceControlRunner, applicationLifecycleService,
                                                              store));
      }
    });
  }

  private static void setSCMFeatureFlag(boolean flag) {
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
    RepositoryConfig namespaceRepo = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink(LINK).setDefaultBranch(DEFAULT_BRANCH).setAuthType(AuthType.PAT)
      .setTokenName(TOKEN_NAME).setUsername(USERNAME).build();

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
      .setTokenName("a new token name")
      .setLink("a new link").build();

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
    String configMissingProvider = buildRepoRequestString(null, LINK, DEFAULT_BRANCH,
                                                          new AuthConfig(AuthType.PAT, TOKEN_NAME, USERNAME),
                                                          null);
    assertResponseCode(400, setRepository(NAME, configMissingProvider));
    assertResponseCode(404, getRepository(NAME));

    // Set the invalid repository that's missing link
    String configMissingLink = buildRepoRequestString(Provider.GITHUB, null, DEFAULT_BRANCH,
                                                      new AuthConfig(AuthType.PAT, TOKEN_NAME, USERNAME),
                                                      null);
    assertResponseCode(400, setRepository(NAME, configMissingLink));
    assertResponseCode(404, getRepository(NAME));

    // Set the invalid repository that's missing auth token name
    String configMissingTokenName = buildRepoRequestString(Provider.GITHUB, LINK, "",
                                                           new AuthConfig(AuthType.PAT, "", USERNAME),
                                                           null);
    assertResponseCode(400, setRepository(NAME, configMissingTokenName));
    assertResponseCode(404, getRepository(NAME));

    // cleanup
    response = deleteNamespace(NAME);
    assertResponseCode(200, response);
  }

  @Test
  public void testFeatureFlagDisabledEndpoint() throws Exception {
    setSCMFeatureFlag(false);

    // Set repository config
    RepositoryConfig namespaceRepo = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink(LINK).setDefaultBranch(DEFAULT_BRANCH).setAuthType(AuthType.PAT)
      .setTokenName(TOKEN_NAME).setUsername(USERNAME).build();

    // Assert the namespace does not exist
    assertResponseCode(403, setRepository(NAME, createRepoRequestString(namespaceRepo)));
    assertResponseCode(403, getRepository(NAME));
    assertResponseCode(403, deleteRepository(NAME));
  }

  @Test
  public void testValidateRepoConfigSuccess() throws Exception {
    RepositoryConfig config = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink(LINK)
      .setDefaultBranch(DEFAULT_BRANCH)
      .setAuthType(AuthType.PAT)
      .setTokenName(TOKEN_NAME)
      .build();

    Mockito.doNothing()
      .when(sourceControlService)
      .validateRepository(Mockito.any(), Mockito.any());

    HttpResponse response = setRepository(NAME, createRepoValidateString(config));
    assertResponseCode(200, response);
  }

  @Test
  public void testValidateRepoConfigFails() throws Exception {
    RepositoryConfig config = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink(LINK)
      .setDefaultBranch(DEFAULT_BRANCH)
      .setAuthType(AuthType.PAT)
      .setTokenName(TOKEN_NAME)
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

    // Push two applications to linked repository
    String commitMessage = "push two apps";
    PushAppResponse expectedAppResponse = new PushAppResponse(appId1.getId(), appId1.getVersion(),
                                                              appId1.getId() + " hash");
    Mockito.doReturn(expectedAppResponse).when(sourceControlService)
      .pushApp(Mockito.any(), Mockito.eq(commitMessage));

    // Assert two app are pushed and updated with source control metadata
    HttpResponse response = pushApplication(NamespaceId.DEFAULT.appReference(appId1.getId()), commitMessage);
    assertResponseCode(200, response);
    PushAppResponse result = readResponse(response, PushAppResponse.class);

    Assert.assertEquals(result, expectedAppResponse);
  }

  @Test
  public void testPushAppNotFound() throws Exception {
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp", "version1");

    // Push two applications to linked repository
    String commitMessage = "push two apps";
    Mockito.doThrow(new NotFoundException("apps not found")).when(sourceControlService)
      .pushApp(Mockito.any(), Mockito.eq(commitMessage));

    // Assert two app are pushed and updated with source control metadata
    HttpResponse response = pushApplication(NamespaceId.DEFAULT.appReference(appId1.getId()), commitMessage);
    assertResponseCode(404, response);
    Assert.assertEquals(response.getResponseBodyAsString(), "apps not found");
  }

  @Test
  public void testPushAppNoChange() throws Exception {
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, "ConfigApp", "version1");

    // Push two applications to linked repository
    String commitMessage = "push two apps";

    Mockito.doThrow(new NoChangesToPushException("No changes for apps to push")).when(sourceControlService)
      .pushApp(Mockito.any(), Mockito.eq(commitMessage));

    // Assert two app are pushed and updated with source control metadata
    HttpResponse response = pushApplication(NamespaceId.DEFAULT.appReference(appId1.getId()), commitMessage);
    assertResponseCode(400, response);
    Assert.assertTrue(response.getResponseBodyAsString().contains("No changes for apps to push"));
  }

  private String buildRepoRequestString(Provider provider, String link, String defaultBranch,
                                        AuthConfig authConfig, @Nullable String pathPrefix) {
    Map<String, Object> authJsonMap = ImmutableMap.of("type", authConfig.getType().toString(),
                                                      "tokenName", authConfig.getTokenName(),
                                                      "username", authConfig.getUsername());
    Map<String, Object> repoString = ImmutableMap.of("provider", provider == null ? "" : provider.toString(),
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
