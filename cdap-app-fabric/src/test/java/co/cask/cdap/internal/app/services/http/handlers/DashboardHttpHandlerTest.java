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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.gateway.handlers.DashboardHttpHandler;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link DashboardHttpHandler}
 */
public class DashboardHttpHandlerTest extends AppFabricTestBase {
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type MAP_STRING_INTEGER_TYPE = new TypeToken<Map<String, Integer>>() { }.getType();
  private static final Type LIST_MAP_STRING_BYTEARRAY_TYPE = new TypeToken<List<Map<String, byte[]>>>() { }.getType();
  private static final Type MAP_STRING_BYTEARRAY_TYPE = new TypeToken<Map<String, byte[]>>() { }.getType();

  @Test
  public void testCleanSlate() throws Exception {
    List<Map<String, byte[]>> dash = getDashboards("mynamespace");
    Assert.assertEquals(0, dash.size());

    String s = createDashboard("mynamespace", 200);
    dash = getDashboards("mynamespace");
    Assert.assertEquals(1, dash.size());

    deleteDashboard("mynamespace", s, 200);
    deleteDashboard("mynamespace", s, 404);

    dash = getDashboards("mynamespace");
    Assert.assertEquals(0, dash.size());
  }

  @Test
  public void testMultiNamespace() throws Exception {
    int maxRuns = 5;
    for (int run = 0; run < maxRuns; run++) {
      int maxNamespace = 10;
      Map<Integer, String> dashboardIds = Maps.newHashMap();
      for (int nsId = 0; nsId < maxNamespace; nsId++) {
        dashboardIds.put(nsId, createDashboard("myspace" + nsId, 200));
      }

      List<Map<String, byte[]>> dashboards;
      for (int nsId = 0; nsId < maxNamespace; nsId++) {
        dashboards = getDashboards("myspace" + nsId);
        Assert.assertEquals(1, dashboards.size());
        Assert.assertEquals(dashboardIds.get(nsId), Bytes.toString(dashboards.get(0).get("id")));
      }

      for (int nsId = 0; nsId < maxNamespace; nsId++) {
        deleteDashboard("myspace" + nsId, dashboardIds.get(nsId), 200);
        deleteDashboard("myspace" + nsId, dashboardIds.get(nsId), 404);
        Assert.assertEquals(0, getDashboards("myspace" + nsId).size());
      }
    }
  }

  @Test
  public void testProperties() throws Exception {
    Map<String, Integer> intMap = Maps.newHashMap();
    intMap.put("k1", 123);
    intMap.put("k2", 324);
    String dash = createDashboard("newspace", GSON.toJson(intMap), 200);
    Map<String, byte[]> contents = GSON.fromJson(getContents("newspace", dash, 200), MAP_STRING_BYTEARRAY_TYPE);
    Assert.assertEquals(2, contents.size());
    Assert.assertEquals(intMap, GSON.fromJson(Bytes.toString(contents.get("config")), MAP_STRING_INTEGER_TYPE));

    Map<String, String> propMap = Maps.newHashMap();
    propMap.put("k2", "value2");
    propMap.put("k1", "value1");
    addProperty("newspace", dash, propMap, 200);
    contents = GSON.fromJson(getContents("newspace", dash, 200), MAP_STRING_BYTEARRAY_TYPE);
    Assert.assertEquals(2, contents.size());
    Assert.assertEquals(propMap, GSON.fromJson(Bytes.toString(contents.get("config")), MAP_STRING_STRING_TYPE));

    propMap.clear();
    propMap.put("m1", "n1");
    String anotherDash = createDashboard("newspace", GSON.toJson(propMap), 200);
    contents = GSON.fromJson(getContents("newspace", anotherDash, 200), MAP_STRING_BYTEARRAY_TYPE);
    Assert.assertEquals(propMap, GSON.fromJson(Bytes.toString(contents.get("config")), MAP_STRING_STRING_TYPE));

    addProperty("newspace", anotherDash, new HashMap<String, String>(), 200);
    contents = GSON.fromJson(getContents("newspace", anotherDash, 200), MAP_STRING_BYTEARRAY_TYPE);
    Assert.assertEquals(Maps.newHashMap(), GSON.fromJson(Bytes.toString(contents.get("config")),
                                                         MAP_STRING_STRING_TYPE));

    String str = "some123 random string!@#";
    createDashboard("space", str, 400);

    deleteDashboard("newspace", dash, 200);
    deleteDashboard("newspace", dash, 404);
    deleteDashboard("newspace", anotherDash, 200);
  }

  @Test
  public void testGetDashboards() throws Exception {
    String dash1 = createDashboard("space1", 200);
    String dash2 = createDashboard("space2", 200);

    List<Map<String, byte[]>> dashList = getDashboards("space1");
    Assert.assertEquals(1, dashList.size());

    deleteDashboard("space1", dash1, 200);
    deleteDashboard("space2", dash2, 200);
  }

  private void addProperty(String namespace, String name, Map<String, String> props, int expectedStatus)
    throws Exception {
    HttpResponse response = doPut(
      String.format("/v3/namespaces/%s/configuration/dashboards/%s", namespace, name), GSON.toJson(props));
    Assert.assertEquals(expectedStatus, response.getStatusLine().getStatusCode());
  }

  private InputStreamReader getContents(String namespace, String name, int expectedStatus) throws Exception {
    HttpResponse response = doGet(
      String.format("/v3/namespaces/%s/configuration/dashboards/%s", namespace, name));
    Assert.assertEquals(expectedStatus, response.getStatusLine().getStatusCode());
    return new InputStreamReader(response.getEntity().getContent());
  }

  private String createDashboard(String namespace, int expectedStatus) throws Exception {
    return createDashboard(namespace, null, expectedStatus);
  }

  private String createDashboard(String namespace, String contents, int expectedStatus) throws Exception {
    HttpResponse response = doPost(String.format("/v3/namespaces/%s/configuration/dashboards", namespace), contents);
    Assert.assertEquals(expectedStatus, response.getStatusLine().getStatusCode());
    if (expectedStatus == HttpResponseStatus.OK.code()) {
      String jsonData = EntityUtils.toString(response.getEntity());
      Map<String, String> idMap = GSON.fromJson(jsonData, MAP_STRING_STRING_TYPE);
      Assert.assertEquals(true, idMap.containsKey("id"));
      return idMap.get("id");
    }
    return null;
  }

  private List<Map<String, byte[]>> getDashboards(String namespace) throws Exception {
    String req = String.format("/v3/namespaces/%s/configuration/dashboards", namespace);
    HttpResponse response = doGet(req);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    return GSON.fromJson(s, LIST_MAP_STRING_BYTEARRAY_TYPE);
  }

  private void deleteDashboard(String namespace, String name, int expectedStatus) throws Exception {
    HttpResponse response = doDelete(String.format("/v3/namespaces/%s/configuration/dashboards/%s", namespace, name));
    Assert.assertEquals(expectedStatus, response.getStatusLine().getStatusCode());
  }
}
