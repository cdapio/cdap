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
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.NamespaceMeta;
import com.google.gson.Gson;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

/**
 * Tests for {@link co.cask.cdap.gateway.handlers.NamespaceHttpHandler}
 */
public class NamespaceHttpHandlerTest extends AppFabricTestBase {

  private static final Gson GSON = new Gson();
  private static final String NAME = "test";
  private static final String DISPLAY_NAME = "displayTest";
  private static final String DESCRIPTION = "test description";
  private static final NamespaceMeta METADATA_VALID = new NamespaceMeta.Builder().setName(NAME)
    .setDisplayName(DISPLAY_NAME).setDescription(DESCRIPTION).build();
  private static final NamespaceMeta METADATA_MISSING_NAME = new NamespaceMeta.Builder()
    .setDisplayName(DISPLAY_NAME).setDescription(DESCRIPTION).build();
  private static final NamespaceMeta METADATA_EMPTY_NAME = new NamespaceMeta.Builder().setName("")
    .setDisplayName(DISPLAY_NAME).setDescription(DESCRIPTION).build();
  private static final NamespaceMeta METADATA_MISSING_DISPLAY_NAME = new NamespaceMeta.Builder().setName(NAME)
    .setDescription(DESCRIPTION).build();
  private static final NamespaceMeta METADATA_EMPTY_DISPLAY_NAME = new NamespaceMeta.Builder().setName(NAME)
    .setDisplayName("").setDescription(DESCRIPTION).build();
  private static final NamespaceMeta METADATA_MISSING_DESCRIPTION = new NamespaceMeta.Builder().setName(NAME)
    .setDisplayName(DISPLAY_NAME).build();
  private static final String METADATA_INVALID_JSON = "invalid";

  private int createNamespace(NamespaceMeta metadata) throws Exception {
    return createNamespace(GSON.toJson(metadata));
  }

  private int createNamespace(String metadata) throws Exception {
    HttpResponse response = doPost(String.format("%s/namespaces", Constants.Gateway.API_VERSION), metadata);
    return response.getStatusLine().getStatusCode();
  }

  private int listNamespace(@Nullable String name) throws Exception {
    StringBuilder api = new StringBuilder(String.format("%s/namespaces", Constants.Gateway.API_VERSION));
    if (null != name) {
      api.append("/").append(name);
    }
    HttpResponse response = doGet(api.toString());
    return response.getStatusLine().getStatusCode();
  }

  private int deleteNamespace(String name) throws Exception {
    HttpResponse response = doDelete(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION, name));
    return response.getStatusLine().getStatusCode();
  }

  @Test
  public void testGetAllNamespaces() throws Exception {
    Assert.assertEquals(200, listNamespace(null));
  }

  @Test
  public void testCreateNamespace() throws Exception {
    Assert.assertEquals(200, createNamespace(METADATA_VALID));
    Assert.assertEquals(200, listNamespace(NAME));
    // test duplicate creation
    Assert.assertEquals(409, createNamespace(METADATA_VALID));
    // cleanup
    Assert.assertEquals(200, deleteNamespace(NAME));
  }

  @Test
  public void testDeleteNamespace() throws Exception {
    // test deleting non-existent namespace
    Assert.assertEquals(404, deleteNamespace("doesnotexist"));
    // setup - create namespace
    Assert.assertEquals(200, createNamespace(METADATA_VALID));
    Assert.assertEquals(200, listNamespace(NAME));
    // test delete
    Assert.assertEquals(200, deleteNamespace(NAME));
  }

  @Test
  public void testCreateValidations() throws Exception {
    // invalid json should error
    Assert.assertEquals(400, createNamespace(METADATA_INVALID_JSON));
    Assert.assertEquals(404, listNamespace(NAME));

    // name must be non-null, non-empty
    Assert.assertEquals(400, createNamespace(METADATA_MISSING_NAME));
    Assert.assertEquals(400, createNamespace(METADATA_EMPTY_NAME));

    // displayName could be null or empty
    Assert.assertEquals(200, createNamespace(METADATA_MISSING_DISPLAY_NAME));
    Assert.assertEquals(200, deleteNamespace(NAME));
    Assert.assertEquals(200, createNamespace(METADATA_EMPTY_DISPLAY_NAME));
    Assert.assertEquals(200, deleteNamespace(NAME));

    // description could be null
    Assert.assertEquals(200, createNamespace(METADATA_MISSING_DESCRIPTION));
    Assert.assertEquals(200, deleteNamespace(NAME));
  }
}
