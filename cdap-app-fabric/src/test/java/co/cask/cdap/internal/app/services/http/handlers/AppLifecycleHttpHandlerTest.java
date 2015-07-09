/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.BloatedWordCountApp;
import co.cask.cdap.ConfigTestApp;
import co.cask.cdap.WordCountApp;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.AppLifecycleHttpHandler;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
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

  @Test
  public void testAppWithConfig() throws Exception {
    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("abc", "def");
    HttpResponse response = deploy(ConfigTestApp.class, "ConfigApp", config);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    JsonObject appDetails = getAppDetails(Constants.DEFAULT_NAMESPACE, "ConfigApp");
    Assert.assertEquals(GSON.toJson(config), appDetails.get("configuration").getAsString());
  }

  @Test
  public void testDeployWithVersion() throws Exception {
    HttpResponse response = deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN,
                                   TEST_NAMESPACE1, "BobApp", "1.2.3");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    JsonObject appDetails = getAppDetails(TEST_NAMESPACE1, "BobApp");
    Assert.assertEquals("1.2.3", appDetails.get("version").getAsString());
    Assert.assertNull(appDetails.get("configuration"));
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
    HttpResponse response = deploy(BloatedWordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
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
    Assert.assertEquals("WordCountApp", result.get("name").getAsString());
    Assert.assertEquals("Application for counting words", result.get("description").getAsString());

    JsonArray streams = result.get("streams").getAsJsonArray();
    Assert.assertEquals(1, streams.size());
    JsonObject stream = streams.get(0).getAsJsonObject();
    Assert.assertEquals("text", stream.get("name").getAsString());

    JsonArray datasets = result.get("datasets").getAsJsonArray();
    Assert.assertEquals(1, datasets.size());
    JsonObject dataset = datasets.get(0).getAsJsonObject();
    Assert.assertEquals("mydataset", dataset.get("name").getAsString());

    JsonArray programs = result.get("programs").getAsJsonArray();
    Assert.assertEquals(6, programs.size());
    JsonObject[] progs = new JsonObject[programs.size()];
    for (int i = 0; i < programs.size(); i++) {
      progs[i] = programs.get(i).getAsJsonObject();
    }
    // sort the programs by name to make this test deterministic
    Arrays.sort(progs, new Comparator<JsonObject>() {
      @Override
      public int compare(JsonObject o1, JsonObject o2) {
        return o1.get("name").getAsString().compareTo(o2.get("name").getAsString());
      }
    });
    int i = 0;
    Assert.assertEquals("Worker", progs[i].get("type").getAsString());
    Assert.assertEquals("LazyGuy", progs[i].get("name").getAsString());
    Assert.assertEquals("nothing to describe", progs[i].get("description").getAsString());
    i++;
    Assert.assertEquals("Workflow", progs[i].get("type").getAsString());
    Assert.assertEquals("SingleStep", progs[i].get("name").getAsString());
    Assert.assertEquals("", progs[i].get("description").getAsString());
    i++;
    Assert.assertEquals("Spark", progs[i].get("type").getAsString());
    Assert.assertEquals("SparklingNothing", progs[i].get("name").getAsString());
    Assert.assertEquals("Spark program that does nothing", progs[i].get("description").getAsString());
    i++;
    Assert.assertEquals("Mapreduce", progs[i].get("type").getAsString());
    Assert.assertEquals("VoidMapReduceJob", progs[i].get("name").getAsString());
    Assert.assertTrue(progs[i].get("description").getAsString().startsWith("Mapreduce that does nothing"));
    i++;
    Assert.assertEquals("Flow", progs[i].get("type").getAsString());
    Assert.assertEquals("WordCountFlow", progs[i].get("name").getAsString());
    Assert.assertEquals("Flow for counting words", progs[i].get("description").getAsString());
    i++;
    Assert.assertEquals("Service", progs[i].get("type").getAsString());
    Assert.assertEquals("WordFrequencyService", progs[i].get("name").getAsString());
    Assert.assertEquals("", progs[i].get("description").getAsString());

    //get and verify app details in testnamespace2
    result = getAppDetails(TEST_NAMESPACE2, appName);
    Assert.assertEquals(appName, result.get("name").getAsString());

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
    Id.Program program = Id.Program.from(TEST_NAMESPACE1, "WordCountApp", ProgramType.FLOW, "WordCountFlow");
    startProgram(program);
    waitState(program, "RUNNING");
    // Try to delete an App while its flow is running
    response = doDelete(getVersionedAPIPath("apps/WordCountApp", Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE1));
    Assert.assertEquals(403, response.getStatusLine().getStatusCode());

    stopProgram(program);
    waitState(program, "STOPPED");

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
