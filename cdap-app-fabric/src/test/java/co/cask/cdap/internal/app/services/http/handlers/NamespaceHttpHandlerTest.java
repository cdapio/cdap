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

import co.cask.cdap.AppForUnrecoverableResetTest;
import co.cask.cdap.AppWithDataset;
import co.cask.cdap.AppWithServices;
import co.cask.cdap.AppWithStreamSizeSchedule;
import co.cask.cdap.DummyBatchTemplate;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.AbstractNamespaceClient;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.gateway.handlers.NamespaceHttpHandler;
import co.cask.cdap.internal.app.runtime.adapter.AdapterService;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceConfig;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;

/**
 * Tests for {@link NamespaceHttpHandler}
 */
public class NamespaceHttpHandlerTest extends AppFabricTestBase {

  private static final String EMPTY = "";
  private static final String NAME_FIELD = "name";
  private static final String DESCRIPTION_FIELD = "description";
  private static final String CONFIG_FIELD = "config";
  private static final String NAME = "test";
  private static final Id.Namespace NAME_ID = Id.Namespace.from(NAME);
  private static final String DESCRIPTION = "test description";
  private static final String METADATA_VALID =
    String.format("{\"%s\":\"%s\", \"%s\":\"%s\"}", NAME_FIELD, NAME, DESCRIPTION_FIELD, DESCRIPTION);
  private static final String METADATA_MISSING_FIELDS = "{}";
  private static final String METADATA_EMPTY_FIELDS = "{\"name\":\"\", \"description\":\"\"}";
  private static final String METADATA_INVALID_JSON = "invalid";
  private static final String INVALID_NAME = "!nv@l*d/";
  private static final String OTHER_NAME = "test1";
  private static final Gson GSON = new Gson();

  private HttpResponse createNamespace(String id) throws Exception {
    return doPut(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, id));
  }

  private HttpResponse createNamespace(String metadata, String id) throws Exception {
    return doPut(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, id), metadata);
  }

  private HttpResponse listAllNamespaces() throws Exception {
    return doGet(String.format("%s/namespaces", Constants.Gateway.API_VERSION_3));
  }

  private HttpResponse getNamespace(String name) throws Exception {
    Preconditions.checkArgument(name != null, "namespace name cannot be null");
    return doGet(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, name));
  }

  private HttpResponse deleteNamespace(String name) throws Exception {
    return doDelete(String.format("%s/unrecoverable/namespaces/%s", Constants.Gateway.API_VERSION_3, name));
  }

  private HttpResponse deleteNamespaceData(String name) throws Exception {
    return doDelete(String.format("%s/unrecoverable/namespaces/%s/datasets", Constants.Gateway.API_VERSION_3, name));
  }

  private HttpResponse setProperties(String id, NamespaceMeta meta) throws Exception {
    return doPut(String.format("%s/namespaces/%s/properties", Constants.Gateway.API_VERSION_3, id),
                 GSON.toJson(meta));
  }

  private void assertResponseCode(int expected, HttpResponse response) {
    Assert.assertEquals(expected, response.getStatusLine().getStatusCode());
  }

  private List<JsonObject> readListResponse(HttpResponse response) throws IOException {
    Type typeToken = new TypeToken<List<JsonObject>>() { }.getType();
    return readResponse(response, typeToken);
  }

  private JsonObject readGetResponse(HttpResponse response) throws IOException {
    Type typeToken = new TypeToken<JsonObject>() { }.getType();
    return readResponse(response, typeToken);
  }

  @Test
  public void testNamespacesValidFlows() throws Exception {
    // get initial namespace list
    HttpResponse response = listAllNamespaces();
    assertResponseCode(200, response);
    List<JsonObject> namespaces = readListResponse(response);
    int initialSize = namespaces.size();
    // create and verify
    response = createNamespace(METADATA_VALID, NAME);
    assertResponseCode(200, response);
    response = listAllNamespaces();
    namespaces = readListResponse(response);
    Assert.assertEquals(initialSize + 1, namespaces.size());
    Assert.assertEquals(NAME, namespaces.get(0).get(NAME_FIELD).getAsString());
    Assert.assertEquals(DESCRIPTION, namespaces.get(0).get(DESCRIPTION_FIELD).getAsString());
    // cleanup
    response = deleteNamespace(NAME);
    assertResponseCode(200, response);
    response = listAllNamespaces();
    namespaces = readListResponse(response);
    Assert.assertEquals(initialSize, namespaces.size());
  }

  @Test
  public void testCreateDuplicate() throws Exception {
    // prepare - create namespace
    HttpResponse response = createNamespace(METADATA_VALID, NAME);
    assertResponseCode(200, response);
    response = getNamespace(NAME);
    JsonObject namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);
    Assert.assertEquals(NAME, namespace.get(NAME_FIELD).getAsString());
    Assert.assertEquals(DESCRIPTION, namespace.get(DESCRIPTION_FIELD).getAsString());

    // create again with the same name
    response = createNamespace(METADATA_EMPTY_FIELDS, NAME);
    // create is idempotent, so response code is 200, but no updates should happen
    assertResponseCode(200, response);
    // check that no updates happened
    response = getNamespace(NAME);
    namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);
    Assert.assertEquals(NAME, namespace.get(NAME_FIELD).getAsString());
    Assert.assertEquals(DESCRIPTION, namespace.get(DESCRIPTION_FIELD).getAsString());
    // cleanup
    response = deleteNamespace(NAME);
    assertResponseCode(200, response);
  }

  @Test
  public void testCreateWithConfig() throws Exception {
    // prepare - create namespace with config in its properties
    String propertiesString = String.format("{\"%s\":\"%s\", \"%s\":\"%s\", \"%s\":%s}",
                                            NAME_FIELD, NAME, DESCRIPTION_FIELD, DESCRIPTION,
                                            CONFIG_FIELD, "{\"scheduler.queue.name\":\"testSchedulerQueueName\"}");
    HttpResponse response = createNamespace(propertiesString, NAME);
    assertResponseCode(200, response);
    response = getNamespace(NAME);
    JsonObject namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);
    Assert.assertEquals(NAME, namespace.get(NAME_FIELD).getAsString());
    Assert.assertEquals(DESCRIPTION, namespace.get(DESCRIPTION_FIELD).getAsString());
    Assert.assertEquals("testSchedulerQueueName",
                        namespace.get(CONFIG_FIELD).getAsJsonObject().get("scheduler.queue.name").getAsString());
    response = deleteNamespace(NAME);
    assertResponseCode(200, response);
  }

  @Test
  public void testInvalidReservedId() throws Exception {
    HttpResponse response = createNamespace(METADATA_VALID, INVALID_NAME);
    assertResponseCode(400, response);
    // '-' is not allowed anymore
    response = createNamespace(METADATA_VALID, "my-namespace");
    assertResponseCode(400, response);
    // 'default' and 'system' are reserved namespaces
    response = createNamespace(METADATA_VALID, Constants.DEFAULT_NAMESPACE);
    assertResponseCode(400, response);
    response = createNamespace(METADATA_VALID, Constants.SYSTEM_NAMESPACE);
    assertResponseCode(400, response);
    // we allow deleting the contents in default namespace. However, the namespace itself should never be deleted
    deploy(AppWithDataset.class, Constants.Gateway.API_VERSION_3_TOKEN, Constants.DEFAULT_NAMESPACE, "AppWithDataSet");
    response = deleteNamespace(Constants.DEFAULT_NAMESPACE);
    assertResponseCode(200, response);
    response = getNamespace(Constants.DEFAULT_NAMESPACE);
    Assert.assertEquals(0, getAppList(Constants.DEFAULT_NAMESPACE).size());
    assertResponseCode(200, response);
    // there is no system namespace
    response = deleteNamespace(Constants.SYSTEM_NAMESPACE);
    assertResponseCode(404, response);
  }

  @Test
  public void testCreateInvalidJson() throws Exception {
    // invalid json should return 400
    HttpResponse response = createNamespace(METADATA_INVALID_JSON, NAME);
    assertResponseCode(400, response);
    // verify
    response = getNamespace(NAME);
    assertResponseCode(404, response);
  }

  @Test
  public void testCreateMissingOrEmptyFields() throws Exception {
    // create with no metadata
    HttpResponse response = createNamespace(NAME);
    assertResponseCode(200, response);
    // verify
    response = getNamespace(NAME);
    JsonObject namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);
    Assert.assertEquals(NAME, namespace.get(NAME_FIELD).getAsString());
    Assert.assertEquals(EMPTY, namespace.get(DESCRIPTION_FIELD).getAsString());
    // cleanup
    response = deleteNamespace(NAME);
    assertResponseCode(200, response);

    // create with missing fields
    response = createNamespace(METADATA_MISSING_FIELDS, NAME);
    assertResponseCode(200, response);
    // verify
    response = getNamespace(NAME);
    namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);
    Assert.assertEquals(NAME, namespace.get(NAME_FIELD).getAsString());
    Assert.assertEquals(EMPTY, namespace.get(DESCRIPTION_FIELD).getAsString());
    // cleanup
    response = deleteNamespace(NAME);
    assertResponseCode(200, response);

    // create with empty fields
    response = createNamespace(METADATA_EMPTY_FIELDS, NAME);
    assertResponseCode(200, response);
    // verify
    response = getNamespace(NAME);
    namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);
    Assert.assertEquals(NAME, namespace.get(NAME_FIELD).getAsString());
    Assert.assertEquals(EMPTY, namespace.get(DESCRIPTION_FIELD).getAsString());
    // cleanup
    response = deleteNamespace(NAME);
    assertResponseCode(200, response);
  }

  @Test
  public void testDeleteAll() throws Exception {
    CConfiguration cConf = getInjector().getInstance(CConfiguration.class);
    // test deleting non-existent namespace
    assertResponseCode(404, deleteNamespace("doesnotexist"));
    assertResponseCode(200, createNamespace(NAME));
    assertResponseCode(200, getNamespace(NAME));
    assertResponseCode(200, createNamespace(OTHER_NAME));
    assertResponseCode(200, getNamespace(OTHER_NAME));

    NamespacedLocationFactory namespacedLocationFactory = getInjector().getInstance(NamespacedLocationFactory.class);
    Location nsLocation = namespacedLocationFactory.get(Id.Namespace.from(NAME));
    Assert.assertTrue(nsLocation.exists());

    DatasetFramework dsFramework = getInjector().getInstance(DatasetFramework.class);
    StreamAdmin streamAdmin = getInjector().getInstance(StreamAdmin.class);

    deploy(AppWithServices.class, Constants.Gateway.API_VERSION_3_TOKEN, NAME);
    deploy(AppWithDataset.class, Constants.Gateway.API_VERSION_3_TOKEN, NAME);
    deploy(AppWithStreamSizeSchedule.class, Constants.Gateway.API_VERSION_3_TOKEN, OTHER_NAME);
    deploy(AppForUnrecoverableResetTest.class, Constants.Gateway.API_VERSION_3_TOKEN, OTHER_NAME);

    // create an adapter
    AdapterService adapterService = getInjector().getInstance(AdapterService.class);
    setupAdapter(DummyBatchTemplate.class);
    adapterService.registerTemplates();
    String adapterName = "myAdapter";
    DummyBatchTemplate.Config config = new DummyBatchTemplate.Config("somestream", "0 0 1 1 *");
    AdapterConfig adapterConfig = new AdapterConfig("description", DummyBatchTemplate.NAME, GSON.toJsonTree(config));
    adapterService.createAdapter(NAME_ID, adapterName, adapterConfig);
    adapterService.startAdapter(NAME_ID, adapterName);

    Id.DatasetInstance myDataset = Id.DatasetInstance.from(NAME, "myds");
    Id.Stream myStream = Id.Stream.from(OTHER_NAME, "stream");

    Assert.assertTrue(dsFramework.hasInstance(myDataset));
    Assert.assertTrue(streamAdmin.exists(myStream));
    Id.Program program = Id.Program.from(NAME_ID, "AppWithServices", ProgramType.SERVICE, "NoOpService");
    startProgram(program);
    boolean resetEnabled = cConf.getBoolean(Constants.Dangerous.UNRECOVERABLE_RESET);
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, false);
    // because unrecoverable reset is disabled
    assertResponseCode(403, deleteNamespace(NAME));
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, resetEnabled);
    // because service is running
    assertResponseCode(409, deleteNamespace(NAME));
    Assert.assertTrue(nsLocation.exists());
    stopProgram(program);
    // because adapter is started
    assertResponseCode(409, deleteNamespace(NAME));
    Assert.assertTrue(nsLocation.exists());
    adapterService.stopAdapter(NAME_ID, adapterName);
    // delete should work now
    assertResponseCode(200, deleteNamespace(NAME));
    Assert.assertFalse(nsLocation.exists());
    Assert.assertFalse(dsFramework.hasInstance(myDataset));
    Assert.assertTrue(streamAdmin.exists(myStream));
    assertResponseCode(200, deleteNamespace(OTHER_NAME));
    Assert.assertFalse(streamAdmin.exists(myStream));
    Assert.assertEquals(0, adapterService.getAdapters(NAME_ID).size());

    // Create the namespace again and deploy the application containing schedules.
    // Application deployment should succeed.
    assertResponseCode(200, createNamespace(OTHER_NAME));
    HttpResponse response = deploy(AppForUnrecoverableResetTest.class, Constants.Gateway.API_VERSION_3_TOKEN,
                                   OTHER_NAME);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    assertResponseCode(200, deleteNamespace(OTHER_NAME));
  }

  @Test
  public void testDeleteDatasetsOnly() throws Exception {
    CConfiguration cConf = getInjector().getInstance(CConfiguration.class);
    // test deleting non-existent namespace
    assertResponseCode(200, createNamespace(NAME));
    assertResponseCode(200, getNamespace(NAME));

    NamespacedLocationFactory namespacedLocationFactory = getInjector().getInstance(NamespacedLocationFactory.class);
    Location nsLocation = namespacedLocationFactory.get(Id.Namespace.from(NAME));
    Assert.assertTrue(nsLocation.exists());

    DatasetFramework dsFramework = getInjector().getInstance(DatasetFramework.class);

    deploy(AppWithServices.class, Constants.Gateway.API_VERSION_3_TOKEN, NAME);
    deploy(AppWithDataset.class, Constants.Gateway.API_VERSION_3_TOKEN, NAME);

    Id.DatasetInstance myDataset = Id.DatasetInstance.from(NAME, "myds");

    Assert.assertTrue(dsFramework.hasInstance(myDataset));
    Id.Program program = Id.Program.from(NAME_ID, "AppWithServices", ProgramType.SERVICE, "NoOpService");
    startProgram(program);
    boolean resetEnabled = cConf.getBoolean(Constants.Dangerous.UNRECOVERABLE_RESET);
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, false);
    // because reset is not enabled
    assertResponseCode(403, deleteNamespaceData(NAME));
    Assert.assertTrue(nsLocation.exists());
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, resetEnabled);
    // because service is running
    assertResponseCode(409, deleteNamespaceData(NAME));
    Assert.assertTrue(nsLocation.exists());
    stopProgram(program);
    assertResponseCode(200, deleteNamespaceData(NAME));
    Assert.assertTrue(nsLocation.exists());
    Assert.assertTrue(getAppList(NAME).size() == 2);
    Assert.assertTrue(getAppDetails(NAME, "AppWithServices").get("name").getAsString().equals("AppWithServices"));
    Assert.assertTrue(getAppDetails(NAME, "AppWithDataSet").get("name").getAsString().equals("AppWithDataSet"));
    assertResponseCode(200, getNamespace(NAME));
    Assert.assertFalse(dsFramework.hasInstance(myDataset));
    assertResponseCode(200, deleteNamespace(NAME));
    assertResponseCode(404, getNamespace(NAME));
  }

  @Test
  public void testNamespaceClient() throws Exception {
    // tests the NamespaceClient's ability to interact with Namespace service/handlers.
    AbstractNamespaceClient namespaceClient = getInjector().getInstance(AbstractNamespaceClient.class);
    // test setup creates two namespaces in @BeforeClass, apart from the default namespace which always exists.
    List<NamespaceMeta> namespaces = namespaceClient.list();
    Assert.assertEquals(3, namespaces.size());
    Set<NamespaceMeta> expectedNamespaces = ImmutableSet.of(Constants.DEFAULT_NAMESPACE_META, TEST_NAMESPACE_META1,
                                                            TEST_NAMESPACE_META2);
    Assert.assertEquals(expectedNamespaces, Sets.newHashSet(namespaces));

    NamespaceMeta namespaceMeta = namespaceClient.get(TEST_NAMESPACE1);
    Assert.assertEquals(TEST_NAMESPACE_META1, namespaceMeta);

    try {
      namespaceClient.get("nonExistentNamespace");
      Assert.fail("Did not expect namespace 'nonExistentNamespace' to exist.");
    } catch (NotFoundException expected) {
    }

    // test create and get
    String fooNamespace = "fooNamespace";
    NamespaceMeta toCreate = new NamespaceMeta.Builder().setName(fooNamespace).build();
    namespaceClient.create(toCreate);
    NamespaceMeta receivedMeta = namespaceClient.get(fooNamespace);
    Assert.assertNotNull(receivedMeta);
    Assert.assertEquals(toCreate, receivedMeta);

    namespaceClient.delete(fooNamespace);
    try {
      namespaceClient.get(fooNamespace);
      Assert.fail("Did not expect namespace '" + fooNamespace + "' to exist after deleting it.");
    } catch (NotFoundException expected) {
    }
  }

  @Test
  public void testProperties() throws Exception {
    // create with no metadata
    HttpResponse response = createNamespace(NAME);
    assertResponseCode(200, response);
    // verify
    response = getNamespace(NAME);
    JsonObject namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);
    Assert.assertEquals(NAME, namespace.get(NAME_FIELD).getAsString());
    Assert.assertEquals(EMPTY, namespace.get(DESCRIPTION_FIELD).getAsString());

    // Update scheduler queue name.
    String nonexistentName = NAME + "nonexistent";
    NamespaceMeta meta = new NamespaceMeta.Builder().setName(nonexistentName).setSchedulerQueueName("prod").build();
    setProperties(NAME, meta);
    // assert that the name in the metadata is ignored (the name from the url should be used, instead
    HttpResponse nonexistentGet = getNamespace(nonexistentName);
    Assert.assertEquals(404, nonexistentGet.getStatusLine().getStatusCode());

    response = getNamespace(NAME);
    namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);

    NamespaceConfig config = GSON.fromJson(namespace.get(CONFIG_FIELD).getAsJsonObject(),
                                                           NamespaceConfig.class);
    Assert.assertEquals("prod", config.getSchedulerQueueName());
    Assert.assertEquals(NAME, namespace.get(NAME_FIELD).getAsString());
    Assert.assertEquals(EMPTY, namespace.get(DESCRIPTION_FIELD).getAsString());

    // Update description
    meta = new NamespaceMeta.Builder().setName(NAME).setDescription("new fancy description").build();
    setProperties(NAME, meta);
    response = getNamespace(NAME);
    namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);

    //verify that the description has changed
    Assert.assertEquals("new fancy description", namespace.get(DESCRIPTION_FIELD).getAsString());
    Assert.assertEquals(NAME, namespace.get(NAME_FIELD).getAsString());

    // verify other properties set earlier has not changed.
    config = GSON.fromJson(namespace.get(CONFIG_FIELD).getAsJsonObject(), NamespaceConfig.class);
    Assert.assertEquals("prod", config.getSchedulerQueueName());

    // cleanup
    response = deleteNamespace(NAME);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }
}
