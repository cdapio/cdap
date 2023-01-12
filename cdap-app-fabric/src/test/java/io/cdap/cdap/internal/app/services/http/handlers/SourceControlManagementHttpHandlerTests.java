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

import com.google.gson.Gson;
import io.cdap.cdap.gateway.handlers.SourceControlManagementHttpHandler;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.Provider;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfigRequest;
import io.cdap.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link SourceControlManagementHttpHandler}
 */
public class SourceControlManagementHttpHandlerTests extends AppFabricTestBase {
  
  private static final String NAME = "test";
  private static final Gson GSON = new Gson();

  private void assertResponseCode(int expected, HttpResponse response) {
    Assert.assertEquals(expected, response.getResponseCode());
  }

  private RepositoryConfig readGetRepositoryResponse(HttpResponse response) {
    return readResponse(response, RepositoryConfig.class);
  }

  @Test
  public void testNamespaceRepository() throws Exception {
    // verify the repository does not exist at the beginning
    HttpResponse response = getRepository(NAME);
    assertResponseCode(404, response);

    // Set repository config
    RepositoryConfig namespaceRepo = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink("example.com").setDefaultBranch("develop").setAuthType(AuthType.PAT)
      .setTokenName("token").setUsername("user").build();

    // Assert the namespace does not exist
    response = setRepository(NAME, createRepoRequestString(namespaceRepo));
    assertResponseCode(404, response);

    // Create the NS
    assertResponseCode(200, createNamespace(NAME));

    // Set and verify the repository
    response = setRepository(NAME, createRepoRequestString(namespaceRepo));
    assertResponseCode(200, response);

    response = getRepository(NAME);
    RepositoryConfig repository = readGetRepositoryResponse(response);
    Assert.assertEquals(namespaceRepo, repository);

    RepositoryConfig newRepoConfig = new RepositoryConfig.Builder(namespaceRepo)
      .setTokenName("a new token name")
      .setLink("a new link").build();
    
    response = setRepository(NAME, createRepoRequestString(newRepoConfig));
    assertResponseCode(200, response);
    response = getRepository(NAME);
    repository = readGetRepositoryResponse(response);

    // verify that the repo config has been updated
    Assert.assertEquals(newRepoConfig, repository);

    // Delete the repository
    assertResponseCode(200, deleteRepository(NAME));
    assertResponseCode(404, getRepository(NAME));

    // cleanup
    response = deleteNamespace(NAME);
    assertResponseCode(200, response);
  }

  private String createRepoRequestString(RepositoryConfig namespaceRepo) {
    return GSON.toJson(new RepositoryConfigRequest(namespaceRepo));
  }
}
