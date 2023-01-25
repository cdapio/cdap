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
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.gateway.handlers.SourceControlHttpHandler;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.Provider;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;

/**
 * Tests for {@link SourceControlHttpHandler}
 */
public class SourceControlHttpHandlerTests extends AppFabricTestBase {
  
  private static final String EMPTY = "";
  private static final String NAME = "test";
  private static final Gson GSON = new Gson();

  private void assertResponseCode(int expected, HttpResponse response) {
    Assert.assertEquals(expected, response.getResponseCode());
  }

  private NamespaceMeta readGetNamespaceResponse(HttpResponse response) {
    Type typeToken = new TypeToken<NamespaceMeta>() { }.getType();
    return readResponse(response, typeToken);
  }

  @Test
  public void testUpdateNamespaceRepository() throws Exception {
    // create a namespace with principal
    String nsPrincipal = "nsCreator/somehost.net@somekdc.net";
    String nsKeytabURI = "some/path";

    NamespaceMeta impNsMeta =
      new NamespaceMeta.Builder().setName(NAME).setPrincipal(nsPrincipal).setKeytabURI(nsKeytabURI).build();
    HttpResponse response = createNamespace(GSON.toJson(impNsMeta), impNsMeta.getName());
    assertResponseCode(200, response);
    // verify
    response = getNamespace(NAME);
    NamespaceMeta namespace = readGetNamespaceResponse(response);
    Assert.assertNotNull(namespace);
    Assert.assertEquals(NAME, namespace.getName());
    Assert.assertEquals(EMPTY, namespace.getDescription());
    Assert.assertNull(namespace.getRepository());

    // Update repository config
    RepositoryConfig namespaceRepo = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink("example.com").setDefaultBranch("develop").setAuthType(AuthType.PAT)
      .setTokenName("token").setUsername("user").build();

    response = updateRepository(NAME, GSON.toJson(namespaceRepo));
    assertResponseCode(200, response);
    response = getNamespace(NAME);
    namespace = readGetNamespaceResponse(response);
    Assert.assertNotNull(namespace);

    // verify that the repo config has changed
    Assert.assertEquals(NAME, namespace.getNamespaceId().getNamespace());
    Assert.assertEquals(namespaceRepo, namespace.getRepository());

    // cleanup
    response = deleteNamespace(NAME);
    assertResponseCode(200, response);
  }

  @Test
  public void testUpdateInvalidNamespaceRepository() throws Exception {
    // create a namespace with principal
    String nsPrincipal = "nsCreator/somehost.net@somekdc.net";
    String nsKeytabURI = "some/path";
    String invalidNamespace = "invalid-name-special?!@#chars";

    NamespaceMeta impNsMeta =
      new NamespaceMeta.Builder().setName(NAME).setPrincipal(nsPrincipal).setKeytabURI(nsKeytabURI).build();
    HttpResponse response = createNamespace(GSON.toJson(impNsMeta), impNsMeta.getName());
    assertResponseCode(200, response);
    // verify
    response = getNamespace(NAME);
    NamespaceMeta namespace = readGetNamespaceResponse(response);
    Assert.assertNotNull(namespace);

    // Invalid namespace name
    NamespaceMeta invalidNSMeta = new NamespaceMeta.Builder(impNsMeta).setName(invalidNamespace).build();
    response = createNamespace(GSON.toJson(invalidNSMeta), invalidNSMeta.getName());
    assertResponseCode(400, response);

    // Update repository with invalid config
    RepositoryConfig namespaceRepo = new RepositoryConfig.Builder().setProvider(null)
      .setLink("example.com").setDefaultBranch("develop").setAuthType(AuthType.PAT)
      .setTokenName("token").setUsername("user").build();

    response = updateRepository(NAME, GSON.toJson(namespaceRepo));
    assertResponseCode(400, response);

    namespaceRepo = new RepositoryConfig.Builder(namespaceRepo)
      .setProvider(Provider.GITHUB).setTokenName(null).build();

    response = updateRepository(NAME, GSON.toJson(namespaceRepo));
    assertResponseCode(400, response);

    // cleanup
    response = deleteNamespace(NAME);
    assertResponseCode(200, response);
  }

  @Test
  public void testDeleteNamespaceRepository() throws Exception {
    // create a namespace with principal
    String nsPrincipal = "nsCreator/somehost.net@somekdc.net";
    String nsKeytabURI = "some/path";
    String invalidNamespace = "invalid-name-special";

    RepositoryConfig namespaceRepo = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink("example.com").setDefaultBranch("master").setAuthType(AuthType.PAT)
      .setTokenName("token").setUsername("user").build();

    NamespaceMeta impNsMeta =
      new NamespaceMeta.Builder().setName(NAME).setPrincipal(nsPrincipal)
        .setKeytabURI(nsKeytabURI).setRepository(namespaceRepo).build();
    HttpResponse response = createNamespace(GSON.toJson(impNsMeta), impNsMeta.getName());
    assertResponseCode(200, response);

    // verify
    response = getNamespace(NAME);
    NamespaceMeta namespace = readGetNamespaceResponse(response);
    Assert.assertNotNull(namespace);
    Assert.assertEquals(NAME, namespace.getName());
    Assert.assertEquals(EMPTY, namespace.getDescription());
    Assert.assertEquals(impNsMeta.getRepository(), namespace.getRepository());

    // Delete repository config
    response = deleteNamespaceRepository(NAME);
    assertResponseCode(200, response);
    response = getNamespace(NAME);
    namespace = readGetNamespaceResponse(response);
    Assert.assertNotNull(namespace);
    // verify that the repo config has been deleted
    Assert.assertNull(namespace.getRepository());

    // Delete repository config again
    response = deleteNamespaceRepository(NAME);

    // Should get error that repository config does not exist
    assertResponseCode(404, response);

    // Delete repository config with invalid namespace
    response = deleteNamespaceRepository(invalidNamespace);
    assertResponseCode(400, response);

    // cleanup
    response = deleteNamespace(NAME);
    assertResponseCode(200, response);
  }
}
