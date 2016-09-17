/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.WordCountApp;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.RouteConfigHttpHandler;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Tests for {@link RouteConfigHttpHandler}.
 */
public class RouteConfigHttpHandlerTest extends AppFabricTestBase {

  private static final String WORDCOUNT_APP_NAME = "WordCountApp";
  private static final String WORDCOUNT_SERVICE_NAME = "WordFrequencyService";

  @Test
  public void testRouteStore() throws Exception {
    // deploy, check the status
    HttpResponse response = deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Map<String, Integer> routes = ImmutableMap.<String, Integer>builder().put("v1", 30).put("v2", 70).build();
    String routeAPI = getVersionedAPIPath(
      String.format("apps/%s/services/%s/routeconfig", WORDCOUNT_APP_NAME, WORDCOUNT_SERVICE_NAME),
      Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    doPut(routeAPI, GSON.toJson(routes));
    String getResult = EntityUtils.toString(doGet(routeAPI).getEntity());
    JsonObject jsonObject = GSON.fromJson(getResult, JsonObject.class);
    Assert.assertNotNull(jsonObject);
    Assert.assertEquals(30, jsonObject.get("v1").getAsInt());
    Assert.assertEquals(70, jsonObject.get("v2").getAsInt());
    doDelete(routeAPI);
    HttpResponse getResponse = doGet(routeAPI);
    getResult = EntityUtils.toString(getResponse.getEntity());
    Assert.assertEquals(200, getResponse.getStatusLine().getStatusCode());
    Assert.assertEquals("{}", getResult);
    Assert.assertEquals(404, doDelete(routeAPI).getStatusLine().getStatusCode());
  }
}
