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
import co.cask.cdap.adapter.AdapterSpecification;
import co.cask.cdap.adapter.Sink;
import co.cask.cdap.adapter.Source;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.AppLifecycleHttpHandler;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.NamespaceMeta;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
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
  private static final String TEST_NAMESPACE1 = "testnamespace1";
  private static final String TEST_NAMESPACE2 = "testnamespace2";
  private static final NamespaceMeta TEST_NAMESPACE_META1 = new NamespaceMeta.Builder()
    .setDisplayName(TEST_NAMESPACE1).setDescription(TEST_NAMESPACE1).build();
  private static final NamespaceMeta TEST_NAMESPACE_META2 = new NamespaceMeta.Builder()
    .setDisplayName(TEST_NAMESPACE2).setDescription(TEST_NAMESPACE2).build();

  private static final Type ADAPTER_SPEC_LIST_TYPE = new TypeToken<List<AdapterSpecification>>() { }.getType();
  private static final Map<String, String> EMPTY_MAP = ImmutableMap.of();

  @BeforeClass
  public static void setup() throws Exception {
    HttpResponse response = doPut(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, TEST_NAMESPACE1),
                                  GSON.toJson(TEST_NAMESPACE_META1));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doPut(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, TEST_NAMESPACE2),
                     GSON.toJson(TEST_NAMESPACE_META2));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  /**
   * Tests deploying an application in a non-existing non-default namespace.
   */
  @Test
  public void testDeployNonExistingNamespace() throws Exception {
    HttpResponse response = deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, "random");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
    Assert.assertEquals("Deploy failed - namespace 'random' does not exist.", readResponse(response));
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

    //make sure testnamespace1 has 1 app
    List<JsonObject> apps = getAppList(TEST_NAMESPACE1);
    Assert.assertEquals(1, apps.size());

    //make sure testnamespace2 has 1 app
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

  private List<JsonObject> getAppList(String namespace) throws Exception {
    HttpResponse response = doGet(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, namespace));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Type typeToken = new TypeToken<List<JsonObject>>() { }.getType();
    return readResponse(response, typeToken);
  }

  private JsonObject getAppDetails(String namespace, String appName) throws Exception {
    HttpResponse response = doGet(getVersionedAPIPath(String.format("apps/%s", appName),
                                                      Constants.Gateway.API_VERSION_3_TOKEN, namespace));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Type typeToken = new TypeToken<JsonObject>() { }.getType();
    return readResponse(response, typeToken);
  }

  /**
   * Tests deleting an application.
   */
  @Test
  public void testDelete() throws Exception {
    //Delete an invalid app
    HttpResponse response = doDelete(getVersionedAPIPath("apps/XYZ", Constants.Gateway.API_VERSION_3_TOKEN,
                                                         TEST_NAMESPACE1));
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
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
                                            TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doDelete(getVersionedAPIPath("apps/WordCountApp/", Constants.Gateway.API_VERSION_3_TOKEN,
                                            TEST_NAMESPACE1));
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testAdapterLifeCycle() throws Exception {
    String namespaceId = Constants.DEFAULT_NAMESPACE;
    String adapterId = "adapterId";
    AdapterSpecification adapterToPut =
      new AdapterSpecification(adapterId, "batchStreamToAvro", ImmutableMap.of("frequency", "1m"),
                               ImmutableSet.of(new Source("someSource", Source.Type.STREAM, EMPTY_MAP)),
                               ImmutableSet.of(new Sink("someSink", Sink.Type.DATASET, EMPTY_MAP)));

    HttpResponse response = listAdapters(namespaceId);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    List<AdapterSpecification> list = readResponse(response, ADAPTER_SPEC_LIST_TYPE);
    Assert.assertTrue(list.isEmpty());

    response = createAdapter(namespaceId, adapterToPut);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = listAdapters(namespaceId);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    list = readResponse(response, ADAPTER_SPEC_LIST_TYPE);
    Assert.assertEquals(1, list.size());
    Assert.assertEquals(adapterToPut, list.get(0));

    response = getAdapter(namespaceId, adapterId);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    AdapterSpecification receivedAdapterSpecification = readResponse(response, AdapterSpecification.class);
    Assert.assertEquals(adapterToPut, receivedAdapterSpecification);

    response = deleteAdapter(namespaceId, adapterId);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = getAdapter(namespaceId, adapterId);
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    response = listAdapters(namespaceId);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    list = readResponse(response, ADAPTER_SPEC_LIST_TYPE);
    Assert.assertTrue(list.isEmpty());
  }

  @Test
  public void testNonexistentAdapter() throws Exception {
    String nonexistentAdapterId = "nonexistentAdapterId";
    HttpResponse response = getAdapter(Constants.DEFAULT_NAMESPACE, nonexistentAdapterId);
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    response = deleteAdapter(Constants.DEFAULT_NAMESPACE, nonexistentAdapterId);
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testMultipleAdapters() throws Exception {
    List<AdapterSpecification> adaptersToPut = ImmutableList.of(
      new AdapterSpecification("adapterId", "batchStreamToAvro", ImmutableMap.of("frequency", "30m"),
                               ImmutableSet.of(new Source("someSource", Source.Type.STREAM, EMPTY_MAP)),
                               ImmutableSet.of(new Sink("someSink", Sink.Type.DATASET, EMPTY_MAP))),

      new AdapterSpecification("otherId", "realtimeStreamToAvro", ImmutableMap.of("frequency", "1h"),
                               ImmutableSet.of(new Source("someSource", Source.Type.STREAM, EMPTY_MAP)),
                               ImmutableSet.of(new Sink("someSink", Sink.Type.DATASET, EMPTY_MAP))));
    for (AdapterSpecification adapterSpec : adaptersToPut) {
      HttpResponse response = createAdapter(Constants.DEFAULT_NAMESPACE, adapterSpec);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    }

    HttpResponse response = listAdapters(Constants.DEFAULT_NAMESPACE);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    List<AdapterSpecification> retrievedAdapters = readResponse(response, ADAPTER_SPEC_LIST_TYPE);
    Assert.assertEquals(adaptersToPut.size(), retrievedAdapters.size());
    Assert.assertEquals(Sets.newHashSet(adaptersToPut), Sets.newHashSet(retrievedAdapters));
  }

  private HttpResponse createAdapter(String namespaceId, AdapterSpecification adapterSpec) throws Exception {
    return createAdapter(namespaceId, GSON.toJson(adapterSpec));
  }

  private HttpResponse createAdapter(String namespaceId, String adapterSpecJson) throws Exception {
    return doPut(String.format("%s/namespaces/%s/adapters",
                               Constants.Gateway.API_VERSION_3, namespaceId), adapterSpecJson);
  }

  private HttpResponse listAdapters(String namespaceId) throws Exception {
    return doGet(String.format("%s/namespaces/%s/adapters",
                               Constants.Gateway.API_VERSION_3, namespaceId));
  }

  private HttpResponse getAdapter(String namespaceId, String adapterId) throws Exception {
//    Preconditions.checkArgument(adapterId != null, "adapterId cannot be null");
    return doGet(String.format("%s/namespaces/%s/adapters/%s",
                               Constants.Gateway.API_VERSION_3, namespaceId, adapterId));
  }

  private HttpResponse deleteAdapter(String namespaceId, String adapterId) throws Exception {
    return doDelete(String.format("%s/namespaces/%s/adapters/%s",
                                  Constants.Gateway.API_VERSION_3, namespaceId, adapterId));
  }




  //TODO: move these elsewhere:
  @Test
  public void testCronConversion() {
    Assert.assertEquals("*/1 * * * ?", AppLifecycleHttpHandler.toCronExpr("1m"));
    Assert.assertEquals("*/52 * * * ?", AppLifecycleHttpHandler.toCronExpr("52m"));
    Assert.assertEquals("0 */4 * * ?", AppLifecycleHttpHandler.toCronExpr("4h"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidExpression() {
    AppLifecycleHttpHandler.toCronExpr("62m");
  }

  @Test
  public void invalidExpression1() {
    AppLifecycleHttpHandler.toCronExpr("am");
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidExpression2() {
    AppLifecycleHttpHandler.toCronExpr("1w");
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidExpression3() {
    AppLifecycleHttpHandler.toCronExpr("1d 1h");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    HttpResponse response = doDelete(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3,
                                                   TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doDelete(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, TEST_NAMESPACE2));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }
}
