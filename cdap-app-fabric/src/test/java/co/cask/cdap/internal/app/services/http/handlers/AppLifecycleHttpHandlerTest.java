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
import com.google.gson.JsonObject;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests for {@link AppLifecycleHttpHandler}
 */
public class AppLifecycleHttpHandlerTest extends AppFabricTestBase {

  /**
   * Tests deploying an application in a non-existing non-default namespace.
   */
  @Test
  public void testDeployNonExistingNamespace() throws Exception {
    HttpResponse response = deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, "random");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
    Assert.assertEquals("Deploy failed - namespace 'random' not found.", readResponse(response));
  }

  /**
   * Tests deploying an application.
   */
  @Test
  public void testDeployValid() throws Exception {
    HttpResponse response = deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  /**
   * Tests deploying an invalid application.
   */
  @Test
  public void testDeployInvalid() throws Exception {
    HttpResponse response = deploy(String.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());
    Assert.assertNotNull(response.getEntity());
    Assert.assertTrue(response.getEntity().getContentLength() > 0);
  }

  /**
   * Tests deploying an application with dataset same name as existing dataset but a different type
   */
  @Test
  public void testDeployFailure() throws Exception {
    HttpResponse response = deploy(AppWithDataset.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertNotNull(response.getEntity());

    response = deploy(AppWithDatasetDuplicate.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());
    Assert.assertNotNull(response.getEntity());
  }

  @Test
  public void testListAndGet() throws Exception {
    final String appName = "AppWithDatasetName";
    //deploy without name to testnamespace1
    HttpResponse response = deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    //deploy with name to testnamespace2
    response = deploy(AppWithDataset.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2, appName);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertNotNull(response.getEntity());

    //verify testnamespace1 has 1 app
    List<JsonObject> apps = getAppList(TEST_NAMESPACE1);
    Assert.assertEquals(1, apps.size());

    //verify testnamespace2 has 1 app
    apps = getAppList(TEST_NAMESPACE2);
    Assert.assertEquals(1, apps.size());

    //get and verify app details in testnamespace1
    JsonObject result = getAppDetails(TEST_NAMESPACE1, "WordCountApp");
    Assert.assertEquals("App", result.get("type").getAsString());
    Assert.assertEquals("WordCountApp", result.get("name").getAsString());

    //get and verify app details in testnamespace2
    result = getAppDetails(TEST_NAMESPACE2, appName);
    Assert.assertEquals(appName, result.get("id").getAsString());

    //delete app in testnamespace1
    response = doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    //delete app in testnamespace2
    response = doDelete(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  /**
   * Tests deleting an application.
   */
  @Test
  public void testDelete() throws Exception {
    // Delete an non-existing app
    HttpResponse response = doDelete(getVersionedAPIPath("apps/XYZ", Constants.Gateway.API_VERSION_3_TOKEN,
                                                         TEST_NAMESPACE1));
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    getRunnableStartStop(TEST_NAMESPACE1, "WordCountApp", "flows", "WordCountFlow", "start");
    waitState(TEST_NAMESPACE1, "WordCountApp", "flows", "WordCountFlow", "RUNNING");
    // Try to delete an App while its flow is running
    response = doDelete(getVersionedAPIPath("apps/WordCountApp", Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE1));
    Assert.assertEquals(403, response.getStatusLine().getStatusCode());

    getRunnableStartStop(TEST_NAMESPACE1, "WordCountApp", "flows", "WordCountFlow", "stop");
    waitState(TEST_NAMESPACE1, "WordCountApp", "flows", "WordCountFlow", "STOPPED");

    // Delete the app in the wrong namespace
    response = doDelete(getVersionedAPIPath("apps/WordCountApp", Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE2));
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    //Delete the App after stopping the flow
    response = doDelete(getVersionedAPIPath("apps/WordCountApp/", Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doDelete(getVersionedAPIPath("apps/WordCountApp/", Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE1));
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
  }
}
