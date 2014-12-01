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
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

/**
 * Tests for {@link co.cask.cdap.gateway.handlers.NamespaceHttpHandler}
 */
public class NamespaceHttpHandlerTest extends AppFabricTestBase {

  private static final String NAME = "test";
  private static final String METADATA = "{\"name\": \"test\", \"displayName\": \"displayTest\", \"description\": " +
    "\"test description\"}";

  private int createNamespace(String name, String metadata) throws Exception {
    HttpResponse response = doPut(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION, name), metadata);
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
    Assert.assertEquals(200, createNamespace(NAME, METADATA));
    Assert.assertEquals(200, listNamespace(NAME));
    // test duplicate creation
    Assert.assertEquals(409, createNamespace(NAME, METADATA));
    // cleanup
    Assert.assertEquals(200, deleteNamespace(NAME));
  }

  @Test
  public void testDeleteNamespace() throws Exception {
    // test deleting non-existent namespace
    Assert.assertEquals(404, deleteNamespace("doesnotexist"));
    // setup - create namespace
    Assert.assertEquals(200, createNamespace(NAME, METADATA));
    Assert.assertEquals(200, listNamespace(NAME));
    // test delete
    Assert.assertEquals(200, deleteNamespace(NAME));
  }
}
