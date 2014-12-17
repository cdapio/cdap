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

import co.cask.cdap.gateway.handlers.DashboardServiceHandler;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link DashboardServiceHandler}.
 */
public class DashboardHttpHandlerTest extends AppFabricTestBase {
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type LIST_STRING_TYPE = new TypeToken<List<String>>() { }.getType();

  @Test
  public void testCleanSlate() throws Exception {
    List<String> dash = getDashboards("myspace");
    Assert.assertEquals(0, dash.size());

    String s = createDashboard("myspace", 200);
    Assert.assertEquals("0", s);

    dash = getDashboards("myspace");
    Assert.assertEquals(1, dash.size());

    deleteDashboard("myspace", "0", 200);
    deleteDashboard("myspace", "0", 404);

    dash = getDashboards("myspace");
    Assert.assertEquals(0, dash.size());
  }

  @Test
  public void testMultiNamespace() throws Exception {
    int maxRuns = 5;
    for (int run = 0; run < maxRuns; run++) {
      int maxNamespace = 10;
      for (int nsId = 0; nsId < maxNamespace; nsId++) {
        createDashboard("myspace" + nsId, 200);
      }

      List<String> dashboards;
      for (int nsId = 0; nsId < maxNamespace; nsId++) {
        dashboards = getDashboards("myspace" + nsId);
        Assert.assertEquals(1, dashboards.size());
        Assert.assertEquals(Integer.toString(run), dashboards.get(0));
      }

      for (int nsId = 0; nsId < maxNamespace; nsId++) {
        deleteDashboard("myspace" + nsId, Integer.toString(run), 200);
        deleteDashboard("myspace" + nsId, Integer.toString(run), 404);
      }
    }
  }

  @Test
  public void testProperties() throws Exception {
    String dash = createDashboard("newspace", 200, "{'k1':'v1', 'k2':'v2'}");
    Map<String, String> contents = getContents("newspace", dash, 200);
    Assert.assertEquals(2, contents.size());
    Assert.assertEquals("v1", contents.get("k1"));
    Assert.assertEquals("v2", contents.get("k2"));

    deleteDashboard("newspace", dash, 200);
    deleteDashboard("newspace", dash, 404);
  }

  private Map<String, String> getContents(String namespace, String name, int expectedStatus) throws Exception {
    HttpResponse response = doGet(String.format("/v3/%s/configuration/dashboard/%s/properties", namespace, name));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    return GSON.fromJson(s, MAP_STRING_STRING_TYPE);
  }

  private String createDashboard(String namespace, int expectedStatus) throws Exception {
    return createDashboard(namespace, expectedStatus, null);
  }

  private String createDashboard(String namespace, int expectedStatus, String contents) throws Exception {
    HttpResponse response = doPost(String.format("/v3/%s/configuration/dashboard", namespace), contents);
    Assert.assertEquals(expectedStatus, response.getStatusLine().getStatusCode());
    return EntityUtils.toString(response.getEntity());
  }

  private List<String> getDashboards(String namespace) throws Exception {
    HttpResponse response = doGet(String.format("/v3/%s/configuration/dashboard", namespace));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    return GSON.fromJson(s, LIST_STRING_TYPE);
  }

  private void deleteDashboard(String namespace, String name, int expectedStatus) throws Exception {
    HttpResponse response = doDelete(String.format("/v3/%s/configuration/dashboard/%s", namespace, name));
    Assert.assertEquals(expectedStatus, response.getStatusLine().getStatusCode());
  }
}
