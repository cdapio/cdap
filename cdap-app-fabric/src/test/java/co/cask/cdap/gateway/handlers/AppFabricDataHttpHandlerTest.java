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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.AppWithDataset;
import co.cask.cdap.AppWithMR;
import co.cask.cdap.AppWithWorker;
import co.cask.cdap.WordCountApp;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import com.google.common.collect.ImmutableMap;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class AppFabricDataHttpHandlerTest extends AppFabricTestBase {

  @After
  public void cleanup() throws Exception {
    HttpResponse response = doPost("/v2/unrecoverable/reset");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testDatasetForApp() throws Exception {
    HttpResponse response = deploy(AppWithMR.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = doGet(getVersionedAPIPath("apps/AppWithMR/datasets",
                                         Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String responseString = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> responseList = GSON.fromJson(responseString, LIST_MAP_STRING_STRING_TYPE);
    Assert.assertTrue(responseList.size() > 0);
  }

  @Test
  public void testGetDatasetsByApp() throws Exception {
    HttpResponse response = deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    // Querying for the datasets in a different namespace should not find them.
    response = doGet(getVersionedAPIPath("apps/WordCountApp/datasets",
                                         Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2));
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    response = doGet(getVersionedAPIPath("apps/WordCountApp/datasets",
                                         Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String responseString = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> responseList = GSON.fromJson(responseString, LIST_MAP_STRING_STRING_TYPE);
    Assert.assertEquals(1, responseList.size());
    ImmutableMap<String, String> expectedDataSets = ImmutableMap.<String, String>builder()
      .put("mydataset", KeyValueTable.class.getName()).build();
    for (Map<String, String> ds : responseList) {
      Assert.assertTrue("problem with dataset " + ds.get("id"), ds.containsKey("id"));
      Assert.assertTrue("problem with dataset " + ds.get("id"), ds.containsKey("name"));
      Assert.assertTrue("problem with dataset " + ds.get("id"), ds.containsKey("classname"));
      Assert.assertTrue("problem with dataset " + ds.get("id"), expectedDataSets.containsKey(ds.get("id")));
      Assert.assertEquals("problem with dataset " + ds.get("id"),
                          expectedDataSets.get(ds.get("id")), ds.get("classname"));
    }
  }

  @Test
  public void testGetFlowsByDataset() throws Exception {
    HttpResponse response = deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    // Querying for the flows in a different namespace should not find them.
    response = doGet(getVersionedAPIPath("data/datasets/mydataset/flows",
                                         Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2));
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    response = doGet(getVersionedAPIPath("data/datasets/mydataset/flows",
                                         Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String responseString = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> responseList = GSON.fromJson(responseString, LIST_MAP_STRING_STRING_TYPE);

    ImmutableMap<String, String> expectedFlow = ImmutableMap.of("type", "Flow",
                                                                "app", "WordCountApp",
                                                                "id", "WordCountFlow",
                                                                "name", "WordCountFlow",
                                                                "description", "Flow for counting words");
    Assert.assertEquals(1, responseList.size());
    Assert.assertEquals(expectedFlow, responseList.get(0));
  }

  @Test
  public void testGetWorkersByDataset() throws Exception {
    HttpResponse response = deploy(AppWithWorker.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = doGet(getVersionedAPIPath(String.format("data/datasets/%s/workers", AppWithWorker.DATASET),
                                         Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE2));
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
    response = doGet(getVersionedAPIPath(String.format("data/datasets/%s/workers", AppWithWorker.DATASET),
                                         Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String responseString = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> responseList = GSON.fromJson(responseString, LIST_MAP_STRING_STRING_TYPE);
    ImmutableMap<String, String> expectedWorker = ImmutableMap.of("type", "Worker",
                                                                  "app", AppWithWorker.NAME,
                                                                  "id", AppWithWorker.WORKER,
                                                                  "name", AppWithWorker.WORKER,
                                                                  "description", AppWithWorker.DESCRIPTION);
    Assert.assertEquals(1, responseList.size());
    Assert.assertEquals(expectedWorker, responseList.get(0));
  }
}
