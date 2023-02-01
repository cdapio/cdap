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
import io.cdap.cdap.gateway.handlers.SourceControlManagementHttpHandler;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.sourcecontrol.AuthConfig;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.Provider;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfigRequest;
import io.cdap.cdap.proto.sourcecontrol.RepositoryMeta;
import io.cdap.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Tests for {@link SourceControlManagementHttpHandler}
 */
public class SourceControlManagementHttpHandlerTests extends AppFabricTestBase {
  
  private static final String NAME = "testNamespace";
  private static final String LINK = "example.com";
  private static final String DEFAULT_BRANCH = "develop";
  private static final String TOKEN_NAME = "test_token_name";
  private static final String USERNAME = "test_user";

  private static final String TEST_FIELD = "test";
  private static final String REPO_FIELD = "repository";
  private static final Gson GSON = new Gson();

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
  public void testInvalidRepoConfig() throws Exception  {
    // Create the NS
    assertResponseCode(200, createNamespace(NAME));
    
    // verify the repository does not exist at the beginning
    HttpResponse response = getRepository(NAME);
    assertResponseCode(404, response);

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
}
