/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.NamespaceHttpHandler;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.NamespaceMeta;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

/**
 * Tests for {@link NamespaceHttpHandler}
 */
public class NamespaceHttpHandlerTest extends AppFabricTestBase {

  private static final Gson GSON = new Gson();
  private static final String ID_FIELD = "id";
  private static final String DISPLAY_NAME_FIELD = "displayName";
  private static final String DESCRIPTION_FIELD = "description";
  private static final String ID = "test";
  private static final String DISPLAY_NAME = "displayTest";
  private static final String DESCRIPTION = "test description";
  private static final NamespaceMeta METADATA_VALID = new NamespaceMeta.Builder().setDisplayName(DISPLAY_NAME)
    .setDescription(DESCRIPTION).build();
  private static final NamespaceMeta METADATA_MISSING_DISPLAY_NAME = new NamespaceMeta.Builder()
    .setDescription(DESCRIPTION).build();
  private static final NamespaceMeta METADATA_EMPTY_DISPLAY_NAME = new NamespaceMeta.Builder()
    .setDisplayName("").setDescription(DESCRIPTION).build();
  private static final NamespaceMeta METADATA_MISSING_DESCRIPTION = new NamespaceMeta.Builder()
    .setDisplayName(DISPLAY_NAME).build();
  private static final String METADATA_INVALID_JSON = "invalid";
  private static final String INVALID_ID = "!nv@l*d/";

  private HttpResponse createNamespace(NamespaceMeta metadata, String id) throws Exception {
    return createNamespace(GSON.toJson(metadata), id);
  }

  private HttpResponse createNamespace(String metadata, String id) throws Exception {
    return doPut(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, id), metadata);
  }

  private HttpResponse listAllNamespaces() throws Exception {
    return doGet(String.format("%s/namespaces", Constants.Gateway.API_VERSION_3));
  }

  private HttpResponse getNamespace(String name) throws Exception {
    Preconditions.checkArgument(name != null, "namespace name cannot be null");
    return doGet(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, name));
  }

  private HttpResponse deleteNamespace(String name) throws Exception {
    return doDelete(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, name));
  }

  private void assertResponseCode(int expected, HttpResponse response) {
    Assert.assertEquals(expected, response.getStatusLine().getStatusCode());
  }

  private List<JsonObject> readListResponse(HttpResponse response) throws IOException {
    Type typeToken = new TypeToken<List<JsonObject>>() { }.getType();
    return readResponse(response, typeToken);
  }

  private JsonObject readGetResponse(HttpResponse response) throws IOException {
    Type typeToken = new TypeToken<JsonObject>() { }.getType();
    return readResponse(response, typeToken);
  }

  @Test
  public void testNamespacesValidFlows() throws Exception {
    // no namespaces initially
    HttpResponse response = listAllNamespaces();
    assertResponseCode(200, response);
    List<JsonObject> namespaces = readListResponse(response);
    Assert.assertEquals(0, namespaces.size());
    // create and verify
    response = createNamespace(METADATA_VALID, ID);
    assertResponseCode(200, response);
    response = listAllNamespaces();
    namespaces = readListResponse(response);
    Assert.assertEquals(1, namespaces.size());
    Assert.assertEquals(ID, namespaces.get(0).get(ID_FIELD).getAsString());
    Assert.assertEquals(DISPLAY_NAME, namespaces.get(0).get(DISPLAY_NAME_FIELD).getAsString());
    Assert.assertEquals(DESCRIPTION, namespaces.get(0).get(DESCRIPTION_FIELD).getAsString());
    // cleanup
    response = deleteNamespace(ID);
    assertResponseCode(200, response);
    response = listAllNamespaces();
    namespaces = readListResponse(response);
    Assert.assertEquals(0, namespaces.size());
  }

  @Test
  public void testCreateDuplicate() throws Exception {
    // prepare - create namespace
    HttpResponse response = createNamespace(METADATA_VALID, ID);
    assertResponseCode(200, response);
    response = getNamespace(ID);
    JsonObject namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);
    Assert.assertEquals(ID, namespace.get(ID_FIELD).getAsString());
    Assert.assertEquals(DISPLAY_NAME, namespace.get(DISPLAY_NAME_FIELD).getAsString());
    Assert.assertEquals(DESCRIPTION, namespace.get(DESCRIPTION_FIELD).getAsString());

    // create again with the same name
    response = createNamespace(METADATA_EMPTY_DISPLAY_NAME, ID);
    assertResponseCode(409, response);
    // check that no updates happened
    response = getNamespace(ID);
    namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);
    Assert.assertEquals(ID, namespace.get(ID_FIELD).getAsString());
    Assert.assertEquals(DISPLAY_NAME, namespace.get(DISPLAY_NAME_FIELD).getAsString());
    Assert.assertEquals(DESCRIPTION, namespace.get(DESCRIPTION_FIELD).getAsString());
    // cleanup
    response = deleteNamespace(ID);
    assertResponseCode(200, response);
  }

  @Test
  public void testInvalidReservedId() throws Exception {
    HttpResponse response = createNamespace(METADATA_VALID, INVALID_ID);
    assertResponseCode(400, response);
    // 'default' and 'system' are reserved namespaces
    response = createNamespace(METADATA_VALID, Constants.DEFAULT_NAMESPACE);
    assertResponseCode(409, response);
    response = createNamespace(METADATA_VALID, Constants.SYSTEM_NAMESPACE);
    assertResponseCode(409, response);
    response = deleteNamespace(Constants.DEFAULT_NAMESPACE);
    assertResponseCode(403, response);
    response = deleteNamespace(Constants.DEFAULT_NAMESPACE);
    assertResponseCode(403, response);
  }

  @Test
  public void testCreateInvalidJson() throws Exception {
    // invalid json should return 400
    HttpResponse response = createNamespace(METADATA_INVALID_JSON, ID);
    assertResponseCode(400, response);
    // verify
    response = getNamespace(ID);
    assertResponseCode(404, response);
  }

  @Test
  public void testCreateMissingOrEmptyDisplayName() throws Exception {
    // create with missing displayName
    HttpResponse response = createNamespace(METADATA_MISSING_DISPLAY_NAME, ID);
    assertResponseCode(200, response);
    // verify
    response = getNamespace(ID);
    JsonObject namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);
    Assert.assertEquals(ID, namespace.get(ID_FIELD).getAsString());
    Assert.assertEquals(ID, namespace.get(DISPLAY_NAME_FIELD).getAsString());
    Assert.assertEquals(DESCRIPTION, namespace.get(DESCRIPTION_FIELD).getAsString());
    // cleanup
    response = deleteNamespace(ID);
    assertResponseCode(200, response);

    // create with empty displayName
    response = createNamespace(METADATA_EMPTY_DISPLAY_NAME, ID);
    assertResponseCode(200, response);
    // verify
    response = getNamespace(ID);
    namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);
    Assert.assertEquals(ID, namespace.get(ID_FIELD).getAsString());
    Assert.assertEquals(ID, namespace.get(DISPLAY_NAME_FIELD).getAsString());
    Assert.assertEquals(DESCRIPTION, namespace.get(DESCRIPTION_FIELD).getAsString());
    // cleanup
    response = deleteNamespace(ID);
    assertResponseCode(200, response);
  }

  @Test
  public void testCreateMissingDescription() throws Exception {
    // create with missing description
    HttpResponse response = createNamespace(METADATA_MISSING_DESCRIPTION, ID);
    assertResponseCode(200, response);
    response = getNamespace(ID);
    JsonObject namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);
    Assert.assertEquals(ID, namespace.get(ID_FIELD).getAsString());
    Assert.assertEquals(DISPLAY_NAME, namespace.get(DISPLAY_NAME_FIELD).getAsString());
    Assert.assertEquals("", namespace.get(DESCRIPTION_FIELD).getAsString());
    response = deleteNamespace(ID);
    assertResponseCode(200, response);
  }

  @Test
  public void testDeleteMissingNamespace() throws Exception {
    // test deleting non-existent namespace
    HttpResponse response = deleteNamespace("doesnotexist");
    assertResponseCode(404, response);
  }
}
