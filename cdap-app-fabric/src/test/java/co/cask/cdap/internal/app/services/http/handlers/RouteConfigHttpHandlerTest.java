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

import co.cask.cdap.ConfigTestApp;
import co.cask.cdap.WordCountApp;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.id.Id;
import co.cask.cdap.gateway.handlers.RouteConfigHttpHandler;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
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
    Id.Artifact artifactId = Id.Artifact.from(new Id.Namespace(TEST_NAMESPACE1), "wordcountapp", VERSION1);
    addAppArtifact(artifactId, WordCountApp.class).getStatusLine().getStatusCode();

    ApplicationId appIdV1 = new ApplicationId(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, "v1");
    ApplicationId appIdV2 = new ApplicationId(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, "v2");
    AppRequest<ConfigTestApp.ConfigClass> request = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), null);
    Assert.assertEquals(200, deploy(appIdV1, request).getStatusLine().getStatusCode());
    Assert.assertEquals(200, deploy(appIdV2, request).getStatusLine().getStatusCode());

    deploy(WordCountApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);

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

    // Invalid Routes should return 400
    routes = ImmutableMap.<String, Integer>builder().put("v1", 50).build();
    HttpResponse response = doPut(routeAPI, GSON.toJson(routes));
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());

    // Valid Routes but non-existing services should return 400
    routes = ImmutableMap.<String, Integer>builder().put("v1", 30).put("v2", 70).build();

    String nonexistNamespaceRouteAPI = getVersionedAPIPath(
      String.format("apps/%s/services/%s/routeconfig", WORDCOUNT_APP_NAME, WORDCOUNT_SERVICE_NAME),
      Constants.Gateway.API_VERSION_3_TOKEN, "_NONEXIST_NAMESPACE_");
    response = doPut(nonexistNamespaceRouteAPI, GSON.toJson(routes));
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());

    String nonexistAppRouteAPI = getVersionedAPIPath(
      String.format("apps/%s/services/%s/routeconfig", "_NONEXIST_APP", WORDCOUNT_SERVICE_NAME),
      Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    response = doPut(nonexistAppRouteAPI, GSON.toJson(routes));
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());

    routes = ImmutableMap.<String, Integer>builder().put("_NONEXIST_v1", 30).put("_NONEXIST_v2", 70).build();
    response = doPut(routeAPI, GSON.toJson(routes));
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());

    // Delete apps
    deleteApp(appIdV1, 200);
    deleteApp(appIdV2, 200);
  }
}
