/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services.http.handlers;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.AppForUnrecoverableResetTest;
import io.cdap.cdap.AppWithDataset;
import io.cdap.cdap.AppWithServices;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.gateway.handlers.NamespaceHttpHandler;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.NamespaceConfig;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.ProgramStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
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

  private void assertResponseCode(int expected, HttpResponse response) {
    Assert.assertEquals(expected, response.getResponseCode());
  }

  private List<JsonObject> readListResponse(HttpResponse response) {
    Type typeToken = new TypeToken<List<JsonObject>>() { }.getType();
    return readResponse(response, typeToken);
  }

  private JsonObject readGetResponse(HttpResponse response) {
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
    // verify that keytab URI cannot be updated since the namespace was created with no principal
    NamespaceMeta meta =
      new NamespaceMeta.Builder().setName(NAME).setKeytabURI("new.keytab").build();
    response = setProperties(NAME, meta);
    assertResponseCode(400, response);
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
    // create the custom namespace location first since we will set the root directory the location needs to exists
    String customRoot = "/custom/root/" + NAME;
    Location customLocation = getInjector().getInstance(LocationFactory.class).create(customRoot);
    Assert.assertTrue(customLocation.mkdirs());
    // prepare - create namespace with config in its properties
    Map<String, String> namespaceConfigString = ImmutableMap.of(NamespaceConfig.SCHEDULER_QUEUE_NAME,
                                                                "testSchedulerQueueName",
                                                                NamespaceConfig.ROOT_DIRECTORY,
                                                                customRoot);

    String propertiesString = GSON.toJson(ImmutableMap.of(NAME_FIELD, NAME, DESCRIPTION_FIELD, DESCRIPTION,
                                                          CONFIG_FIELD, namespaceConfigString));

    HttpResponse response = createNamespace(propertiesString, NAME);
    assertResponseCode(200, response);
    response = getNamespace(NAME);
    JsonObject namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);
    Assert.assertEquals(NAME, namespace.get(NAME_FIELD).getAsString());
    Assert.assertEquals(DESCRIPTION, namespace.get(DESCRIPTION_FIELD).getAsString());
    Assert.assertEquals("testSchedulerQueueName",
                        namespace.get(CONFIG_FIELD).getAsJsonObject().get("scheduler.queue.name").getAsString());
    Assert.assertEquals(customRoot,
                        namespace.get(CONFIG_FIELD).getAsJsonObject().get("root.directory").getAsString());
    response = deleteNamespace(NAME);
    assertResponseCode(200, response);
    // check that the custom location for the namespace was not deleted while deleting namespace
    Assert.assertTrue(customLocation.exists());
    // delete it manually
    Assert.assertTrue(customLocation.delete(true));
  }

  @Test
  public void testInvalidReservedId() throws Exception {
    HttpResponse response = createNamespace(METADATA_VALID, INVALID_NAME);
    assertResponseCode(400, response);
    // '-' is not allowed anymore
    response = createNamespace(METADATA_VALID, "my-namespace");
    assertResponseCode(400, response);
    // 'default' and 'system' are reserved namespaces
    response = createNamespace(METADATA_VALID, Id.Namespace.DEFAULT.getId());
    assertResponseCode(400, response);
    response = createNamespace(METADATA_VALID, Id.Namespace.SYSTEM.getId());
    assertResponseCode(400, response);
    // we allow deleting the contents in default namespace. However, the namespace itself should never be deleted
    deploy(AppWithDataset.class, 200);
    response = deleteNamespace(Id.Namespace.DEFAULT.getId());
    assertResponseCode(200, response);
    response = getNamespace(Id.Namespace.DEFAULT.getId());
    Assert.assertEquals(0, getAppList(Id.Namespace.DEFAULT.getId()).size());
    assertResponseCode(200, response);
    // there is no system namespace
    response = deleteNamespace(Id.Namespace.SYSTEM.getId());
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

    NamespacePathLocator namespacePathLocator = getInjector().getInstance(NamespacePathLocator.class);
    Location nsLocation = namespacePathLocator.get(new NamespaceId(NAME));
    Assert.assertTrue(nsLocation.exists());

    DatasetFramework dsFramework = getInjector().getInstance(DatasetFramework.class);

    deploy(AppWithServices.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, NAME);
    deploy(AppWithDataset.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, NAME);
    deploy(AppForUnrecoverableResetTest.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, OTHER_NAME);

    DatasetId myDataset = new DatasetId(NAME, "myds");

    Assert.assertTrue(dsFramework.hasInstance(myDataset));
    Id.Program program = Id.Program.from(NAME_ID, "AppWithServices", ProgramType.SERVICE, "NoOpService");
    startProgram(program);
    waitState(program, ProgramStatus.RUNNING.name());
    boolean resetEnabled = cConf.getBoolean(Constants.Dangerous.UNRECOVERABLE_RESET);
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, false);
    // because unrecoverable reset is disabled
    assertResponseCode(403, deleteNamespace(NAME));
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, resetEnabled);
    // because service is running
    assertResponseCode(409, deleteNamespace(NAME));
    Assert.assertTrue(nsLocation.exists());
    stopProgram(program);
    waitState(program, ProgramStatus.STOPPED.name());
    // delete should work now
    assertResponseCode(200, deleteNamespace(NAME));
    Assert.assertFalse(nsLocation.exists());
    Assert.assertFalse(dsFramework.hasInstance(myDataset));
    assertResponseCode(200, deleteNamespace(OTHER_NAME));

    // Create the namespace again and deploy the application containing schedules.
    // Application deployment should succeed.
    assertResponseCode(200, createNamespace(OTHER_NAME));
    deploy(AppForUnrecoverableResetTest.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, OTHER_NAME);
    assertResponseCode(200, deleteNamespace(OTHER_NAME));
  }

  @Test
  public void testDeleteDatasetsOnly() throws Exception {
    CConfiguration cConf = getInjector().getInstance(CConfiguration.class);
    // test deleting non-existent namespace
    assertResponseCode(200, createNamespace(NAME));
    assertResponseCode(200, getNamespace(NAME));

    NamespacePathLocator namespacePathLocator = getInjector().getInstance(NamespacePathLocator.class);
    Location nsLocation = namespacePathLocator.get(new NamespaceId(NAME));
    Assert.assertTrue(nsLocation.exists());

    DatasetFramework dsFramework = getInjector().getInstance(DatasetFramework.class);

    deploy(AppWithServices.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, NAME);
    deploy(AppWithDataset.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, NAME);

    DatasetId myDataset = new DatasetId(NAME, "myds");

    Assert.assertTrue(dsFramework.hasInstance(myDataset));
    Id.Program program = Id.Program.from(NAME_ID, "AppWithServices", ProgramType.SERVICE, "NoOpService");
    startProgram(program);
    waitState(program, ProgramStatus.RUNNING.name());
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
    waitState(program, ProgramStatus.STOPPED.name());
    assertResponseCode(200, deleteNamespaceData(NAME));
    Assert.assertTrue(nsLocation.exists());
    Assert.assertEquals(2, getAppList(NAME).size());
    Assert.assertEquals("AppWithServices", getAppDetails(NAME, "AppWithServices").get("name").getAsString());
    Assert.assertEquals(AppWithDataset.class.getSimpleName(),
                        getAppDetails(NAME, AppWithDataset.class.getSimpleName()).get("name").getAsString());
    assertResponseCode(200, getNamespace(NAME));
    Assert.assertFalse(dsFramework.hasInstance(myDataset));
    assertResponseCode(200, deleteNamespace(NAME));
    assertResponseCode(404, getNamespace(NAME));
  }

  @Test
  public void testNamespaceClient() throws Exception {
    // tests the NamespaceClient's ability to interact with Namespace service/handlers.
    NamespaceAdmin namespaceClient = getInjector().getInstance(NamespaceAdmin.class);
    // test setup creates two namespaces in @BeforeClass, apart from the default namespace which always exists.
    List<NamespaceMeta> namespaces = namespaceClient.list();
    Assert.assertEquals(3, namespaces.size());
    Set<NamespaceMeta> expectedNamespaces = ImmutableSet.of(NamespaceMeta.DEFAULT, TEST_NAMESPACE_META1,
                                                            TEST_NAMESPACE_META2);
    for (NamespaceMeta actual : namespaces) {
      actual = new NamespaceMeta.Builder(actual).setGeneration(0L).build();
      Assert.assertTrue(expectedNamespaces.contains(actual));
    }

    NamespaceMeta namespaceMeta = namespaceClient.get(new NamespaceId(TEST_NAMESPACE1));
    namespaceMeta = new NamespaceMeta.Builder(namespaceMeta).setGeneration(0L).build();
    Assert.assertEquals(TEST_NAMESPACE_META1, namespaceMeta);

    try {
      namespaceClient.get(new NamespaceId("nonExistentNamespace"));
      Assert.fail("Did not expect namespace 'nonExistentNamespace' to exist.");
    } catch (NotFoundException expected) {
      // expected
    }

    // test create and get
    NamespaceId fooNamespace = new NamespaceId("fooNamespace");
    NamespaceMeta toCreate = new NamespaceMeta.Builder().setName(fooNamespace).build();
    namespaceClient.create(toCreate);
    NamespaceMeta receivedMeta = namespaceClient.get(fooNamespace);
    Assert.assertNotNull(receivedMeta);
    Assert.assertEquals(toCreate, new NamespaceMeta.Builder(receivedMeta).setGeneration(0L).build());

    namespaceClient.delete(fooNamespace);
    try {
      namespaceClient.get(fooNamespace);
      Assert.fail("Did not expect namespace '" + fooNamespace + "' to exist after deleting it.");
    } catch (NotFoundException expected) {
      // expected
    }
  }

  @Test
  public void testProperties() throws Exception {
    // create a namespace with principal
    String nsPrincipal = "nsCreator/somehost.net@somekdc.net";
    String nsKeytabURI = "some/path";
    NamespaceMeta impNsMeta =
      new NamespaceMeta.Builder().setName(NAME).setPrincipal(nsPrincipal).setKeytabURI(nsKeytabURI).build();
    HttpResponse response = createNamespace(GSON.toJson(impNsMeta), impNsMeta.getName());
    assertResponseCode(200, response);
    // verify
    response = getNamespace(NAME);
    JsonObject namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);
    Assert.assertEquals(NAME, namespace.get(NAME_FIELD).getAsString());
    Assert.assertEquals(EMPTY, namespace.get(DESCRIPTION_FIELD).getAsString());

    // Update scheduler queue name.
    String nonexistentName = NAME + "nonexistent";
    NamespaceMeta meta =
      new NamespaceMeta.Builder(impNsMeta).setName(nonexistentName).setSchedulerQueueName("prod").build();
    setProperties(NAME, meta);
    // assert that the name in the metadata is ignored (the name from the url should be used, instead
    HttpResponse nonexistentGet = getNamespace(nonexistentName);
    Assert.assertEquals(404, nonexistentGet.getResponseCode());

    response = getNamespace(NAME);
    namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);

    NamespaceConfig config = GSON.fromJson(namespace.get(CONFIG_FIELD).getAsJsonObject(),
                                           NamespaceConfig.class);
    Assert.assertEquals("prod", config.getSchedulerQueueName());
    Assert.assertEquals(NAME, namespace.get(NAME_FIELD).getAsString());
    Assert.assertEquals(EMPTY, namespace.get(DESCRIPTION_FIELD).getAsString());

    // Update description
    meta = new NamespaceMeta.Builder(impNsMeta).setName(NAME).setDescription("new fancy description").build();
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

    // verify updating keytab URI with version initialized
    setProperties(NAME, new NamespaceMeta.Builder(impNsMeta).setKeytabURI("new/url").build());
    response = getNamespace(NAME);
    namespace = readGetResponse(response);
    Assert.assertNotNull(namespace);
    config = GSON.fromJson(namespace.get(CONFIG_FIELD).getAsJsonObject(), NamespaceConfig.class);
    // verify that the uri has changed
    Assert.assertEquals("new/url", config.getKeytabURI());
    // cleanup
    response = deleteNamespace(NAME);
    Assert.assertEquals(200, response.getResponseCode());
  }
}
