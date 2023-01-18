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
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.gateway.handlers.SourceControlHttpHandler;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.NamespaceRepositoryConfig;
import io.cdap.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;

/**
 * Tests for {@link SourceControlHttpHandler}
 */
public class SourceControlHttpHandlerTests  extends AppFabricTestBase {
  
  private static final String EMPTY = "";
  private static final String NAME_FIELD = "name";
  private static final String DESCRIPTION_FIELD = "description";
  private static final String REPOSITORY_FIELD = "repository";
  private static final String NAME = "test";
  private static final Gson GSON = new Gson();

  private void assertResponseCode(int expected, HttpResponse response) {
    Assert.assertEquals(expected, response.getResponseCode());
  }

  private JsonObject readGetResponse(HttpResponse response) {
    Type typeToken = new TypeToken<JsonObject>() { }.getType();
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
    JsonObject namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);
    Assert.assertEquals(NAME, namespace.get(NAME_FIELD).getAsString());
    Assert.assertEquals(EMPTY, namespace.get(DESCRIPTION_FIELD).getAsString());
    Assert.assertEquals("{}", namespace.get(REPOSITORY_FIELD).toString());

    // Update repository config
    NamespaceRepositoryConfig namespaceRepo = new NamespaceRepositoryConfig(
      ImmutableMap.of(NamespaceRepositoryConfig.PROVIDER, "github",
                      NamespaceRepositoryConfig.LINK, "example.com",
                      NamespaceRepositoryConfig.AUTH_TYPE, "OAuth",
                      NamespaceRepositoryConfig.DEFAULT_BRANCH, "develop"));
    response = updateRepository(NAME, namespaceRepo);
    assertResponseCode(200, response);
    response = getNamespace(NAME);
    namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);

    // verify that the repo config has changed
    Assert.assertEquals(NAME, namespace.get(NAME_FIELD).getAsString());
    Assert.assertEquals("github",
                        namespace.get(REPOSITORY_FIELD).getAsJsonObject()
                          .get(NamespaceRepositoryConfig.PROVIDER).getAsString());
    Assert.assertEquals("example.com",
                        namespace.get(REPOSITORY_FIELD).getAsJsonObject()
                          .get(NamespaceRepositoryConfig.LINK).getAsString());
    Assert.assertEquals("OAuth",
                        namespace.get(REPOSITORY_FIELD).getAsJsonObject()
                          .get(NamespaceRepositoryConfig.AUTH_TYPE).getAsString());
    Assert.assertEquals("develop",
                        namespace.get(REPOSITORY_FIELD).getAsJsonObject()
                          .get(NamespaceRepositoryConfig.DEFAULT_BRANCH).getAsString());

    // cleanup
    response = deleteNamespace(NAME);
    assertResponseCode(200, response);
  }

  @Test
  public void testDeleteNamespaceRepository() throws Exception {
    // create a namespace with principal
    String nsPrincipal = "nsCreator/somehost.net@somekdc.net";
    String nsKeytabURI = "some/path";

    NamespaceRepositoryConfig namespaceRepo = new NamespaceRepositoryConfig(
      ImmutableMap.of(NamespaceRepositoryConfig.PROVIDER, "github",
                      NamespaceRepositoryConfig.LINK, "example.com",
                      NamespaceRepositoryConfig.AUTH_TYPE, "PAT",
                      NamespaceRepositoryConfig.DEFAULT_BRANCH, "master"));
    NamespaceMeta impNsMeta =
      new NamespaceMeta.Builder().setName(NAME).setPrincipal(nsPrincipal)
        .setKeytabURI(nsKeytabURI).setRepoConfig(namespaceRepo).build();
    HttpResponse response = createNamespace(GSON.toJson(impNsMeta), impNsMeta.getName());
    assertResponseCode(200, response);
    // verify
    response = getNamespace(NAME);
    JsonObject namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);
    Assert.assertEquals(NAME, namespace.get(NAME_FIELD).getAsString());
    Assert.assertEquals(EMPTY, namespace.get(DESCRIPTION_FIELD).getAsString());
    Assert.assertEquals("github",
                        namespace.get(REPOSITORY_FIELD).getAsJsonObject()
                          .get(NamespaceRepositoryConfig.PROVIDER).getAsString());
    Assert.assertEquals("example.com",
                        namespace.get(REPOSITORY_FIELD).getAsJsonObject()
                          .get(NamespaceRepositoryConfig.LINK).getAsString());
    Assert.assertEquals("PAT",
                        namespace.get(REPOSITORY_FIELD).getAsJsonObject()
                          .get(NamespaceRepositoryConfig.AUTH_TYPE).getAsString());
    Assert.assertEquals("master",
                        namespace.get(REPOSITORY_FIELD).getAsJsonObject()
                          .get(NamespaceRepositoryConfig.DEFAULT_BRANCH).getAsString());

    // Delete repository config
    response = deleteNamespaceRepository(NAME);
    assertResponseCode(200, response);
    response = getNamespace(NAME);
    namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);
    // verify that the repo config has been deleted
    Assert.assertEquals("{}", namespace.get(REPOSITORY_FIELD).toString());

    // Delete repository config again
    response = deleteNamespaceRepository(NAME);

    // Should get error that repository config does not exist
    assertResponseCode(404, response);

    // cleanup
    response = deleteNamespace(NAME);
    assertResponseCode(200, response);
  }
}
