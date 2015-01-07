/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.gateway.handlers.DashboardHttpHandler;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link DashboardHttpHandler}
 */
public class DashboardHttpHandlerTest extends AppFabricTestBase {
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  @Test
  public void testCleanSlate() throws Exception {
    JsonElement dash = getDashboards("mynamespace");
    Assert.assertTrue(dash.isJsonArray());
    Assert.assertEquals(0, dash.getAsJsonArray().size());

    String s = createDashboard("mynamespace", 200);
    dash = getDashboards("mynamespace");
    Assert.assertTrue(dash.isJsonArray());
    Assert.assertEquals(1, dash.getAsJsonArray().size());

    deleteDashboard("mynamespace", s, 200);
    deleteDashboard("mynamespace", s, 404);

    dash = getDashboards("mynamespace");
    Assert.assertTrue(dash.isJsonArray());
    Assert.assertEquals(0, dash.getAsJsonArray().size());
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

      JsonArray dashboards;
      for (int nsId = 0; nsId < maxNamespace; nsId++) {
        dashboards = getDashboards("myspace" + nsId).getAsJsonArray();
        Assert.assertEquals(1, dashboards.size());
        Assert.assertEquals(dashboardIds.get(nsId), dashboards.get(0).getAsJsonObject().get("id").getAsString());
      }

      for (int nsId = 0; nsId < maxNamespace; nsId++) {
        deleteDashboard("myspace" + nsId, dashboardIds.get(nsId), 200);
        deleteDashboard("myspace" + nsId, dashboardIds.get(nsId), 404);
        Assert.assertEquals(0, getDashboards("myspace" + nsId).getAsJsonArray().size());
      }
    }
  }

  @Test
  public void testProperties() throws Exception {
    Map<String, Integer> intMap = Maps.newHashMap();
    intMap.put("k1", 123);
    intMap.put("k2", 324);
    String dash = createDashboard("newspace", GSON.toJson(intMap), 200);
    JsonObject jsonObject = getContents("newspace", dash, 200).getAsJsonObject().get("config").getAsJsonObject();
    Assert.assertEquals(2, jsonObject.entrySet().size());
    Assert.assertEquals(123, jsonObject.get("k1").getAsInt());
    Assert.assertEquals(324, jsonObject.get("k2").getAsInt());

    Map<String, String> propMap = Maps.newHashMap();
    propMap.put("k2", "value2");
    propMap.put("k1", "value1");
    addProperty("newspace", dash, propMap, 200);
    jsonObject = getContents("newspace", dash, 200).getAsJsonObject().get("config").getAsJsonObject();
    Assert.assertEquals(2, jsonObject.entrySet().size());
    Assert.assertEquals("value1", jsonObject.get("k1").getAsString());
    Assert.assertEquals("value2", jsonObject.get("k2").getAsString());

    propMap.clear();
    propMap.put("m1", "n1");
    String anotherDash = createDashboard("newspace", GSON.toJson(propMap), 200);
    jsonObject = getContents("newspace", anotherDash, 200).getAsJsonObject().get("config").getAsJsonObject();
    Assert.assertEquals(1, jsonObject.entrySet().size());
    Assert.assertEquals("n1", jsonObject.get("m1").getAsString());

    addProperty("newspace", anotherDash, new HashMap<String, String>(), 200);
    jsonObject = getContents("newspace", anotherDash, 200).getAsJsonObject().get("config").getAsJsonObject();
    Assert.assertEquals(0, jsonObject.entrySet().size());

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

    JsonArray dashList = getDashboards("space1").getAsJsonArray();
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

  private JsonElement getContents(String namespace, String name, int expectedStatus) throws Exception {
    HttpResponse response = doGet(
      String.format("/v3/namespaces/%s/configuration/dashboards/%s", namespace, name));
    Assert.assertEquals(expectedStatus, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    return new JsonParser().parse(s);
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
      Assert.assertEquals(1, idMap.size());
      Assert.assertEquals(true, idMap.containsKey("id"));
      return idMap.get("id");
    }
    return null;
  }

  private JsonElement getDashboards(String namespace) throws Exception {
    String req = String.format("/v3/namespaces/%s/configuration/dashboards", namespace);
    HttpResponse response = doGet(req);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    return new JsonParser().parse(s);
  }

  private void deleteDashboard(String namespace, String name, int expectedStatus) throws Exception {
    HttpResponse response = doDelete(String.format("/v3/namespaces/%s/configuration/dashboards/%s", namespace, name));
    Assert.assertEquals(expectedStatus, response.getStatusLine().getStatusCode());
  }
}
