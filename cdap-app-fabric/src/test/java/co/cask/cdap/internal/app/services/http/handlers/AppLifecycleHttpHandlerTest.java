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

import co.cask.cdap.AppWithDataset;
import co.cask.cdap.AppWithDatasetDuplicate;
import co.cask.cdap.WordCountApp;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.AppLifecycleHttpHandler;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.NamespaceMeta;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link AppLifecycleHttpHandler}
 */
public class AppLifecycleHttpHandlerTest extends AppFabricTestBase {
  private static final Gson GSON = new Gson();
  private static final String TEST_NAMESPACE = "testnamespace";
  private static final NamespaceMeta TEST_NAMESPACE_META = new NamespaceMeta.Builder().setName(TEST_NAMESPACE)
    .setDisplayName(TEST_NAMESPACE).setDescription(TEST_NAMESPACE).build();

  private String getDeploymentStatus() throws Exception {
    HttpResponse response = doGet(getVersionedAPIPath("deploy/status/", Constants.Gateway.API_VERSION_3_TOKEN,
                                                      TEST_NAMESPACE));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Map<String, String> o = new Gson().fromJson(s, new TypeToken<Map<String, String>>() { }.getType());
    return o.get("status");
  }

  @BeforeClass
  public static void setup() throws Exception {
    HttpResponse response = doPut(String.format("%s/namespaces", Constants.Gateway.API_VERSION_3),
                                  GSON.toJson(TEST_NAMESPACE_META));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  /**
   * Tests deploying an application in a non-existing non-default namespace.
   */
  @Test
  public void testDeployNonExistingNamespace() throws Exception {
    HttpResponse response = deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, "random");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
  }

  /**
   * Tests deploying an application.
   */
  @Test
  public void testDeployValid() throws Exception {
    HttpResponse response = deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertEquals("DEPLOYED", getDeploymentStatus());

    response = doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  /**
   * Tests deploying an invalid application.
   */
  @Test
  public void testDeployInvalid() throws Exception {
    HttpResponse response = deploy(String.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE);
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());
    Assert.assertNotNull(response.getEntity());
    Assert.assertTrue(response.getEntity().getContentLength() > 0);
  }

  /**
   * Tests deploying an application with dataset same name as existing dataset but a different type
   */
  @Test
  public void testDeployFailure() throws Exception {
    HttpResponse response = deploy(AppWithDataset.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertNotNull(response.getEntity());

    response = deploy(AppWithDatasetDuplicate.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE);
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());
    Assert.assertNotNull(response.getEntity());
  }

  @Test
  public void testListAndGet() throws Exception {
    final String appName = "AppWithDatasetName";
    //deploy without name
    HttpResponse response = deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertEquals("DEPLOYED", getDeploymentStatus());

    //deploy with name
    response = deploy(AppWithDataset.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE, appName);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertNotNull(response.getEntity());

    response = doGet(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Type typeToken = new TypeToken<List<JsonObject>>() { }.getType();
    List<JsonObject> apps = readResponse(response, typeToken);
    Assert.assertEquals(2, apps.size());

    response = doGet(getVersionedAPIPath("apps/WordCountApp", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    typeToken = new TypeToken<JsonObject>() { }.getType();
    JsonObject result = readResponse(response, typeToken);
    Assert.assertEquals("App", result.get("type").getAsString());
    Assert.assertEquals("WordCountApp", result.get("name").getAsString());

    response = doGet(getVersionedAPIPath("apps/" + appName, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    typeToken = new TypeToken<JsonObject>() { }.getType();
    result = readResponse(response, typeToken);
    Assert.assertEquals(appName, result.get("id").getAsString());

    response = doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  /**
   * Tests deleting an application.
   */
  @Test
  public void testDelete() throws Exception {
    //Delete an invalid app
    HttpResponse response = doDelete(getVersionedAPIPath("apps/XYZ", Constants.Gateway.API_VERSION_3_TOKEN,
                                                         TEST_NAMESPACE));
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE);
    //TODO: Enable these verifications after their v3 APIs are implemented.
    //getRunnableStartStop("flows", "WordCountApp", "WordCountFlow", "start");
    //waitState("flows", "WordCountApp", "WordCountFlow", "RUNNING");
    //Try to delete an App while its flow is running
    //response = doDelete("/v2/apps/WordCountApp");
    //Assert.assertEquals(403, response.getStatusLine().getStatusCode());

    //TODO: Enable these verifications after their v3 APIs are implemented.
    // getRunnableStartStop("flows", "WordCountApp", "WordCountFlow", "stop");
    // waitState("flows", "WordCountApp", "WordCountFlow", "STOPPED");
    //Delete the App after stopping the flow
    response = doDelete(getVersionedAPIPath("apps/WordCountApp/", Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doDelete(getVersionedAPIPath("apps/WordCountApp/", Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE));
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    HttpResponse response = doDelete(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3,
                                                   TEST_NAMESPACE));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }
}
