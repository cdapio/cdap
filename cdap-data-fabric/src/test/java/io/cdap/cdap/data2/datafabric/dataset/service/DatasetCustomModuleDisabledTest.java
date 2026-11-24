/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.data2.datafabric.dataset.service;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.proto.DatasetModuleMeta;
import io.cdap.cdap.proto.DatasetTypeMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpStatus;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that the dataset service and the dataset op executor do not load any module jar when
 * {@link Constants.Dataset#CUSTOM_MODULE_ENABLED} is set to false.
 */
public class DatasetCustomModuleDisabledTest extends DatasetServiceTestBase {

  private static final Gson GSON = new Gson();
  private static final String CUSTOM_TYPE = "customType";

  @BeforeClass
  public static void setup() throws Exception {
    locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    CConfiguration cConf = createCconf();
    cConf.setBoolean(Constants.Dataset.CUSTOM_MODULE_ENABLED, false);
    initializeAndStartService(cConf);
  }

  @Test
  public void testAddModuleIsForbidden() throws Exception {
    HttpResponse response = deployModule("module1", TestModule1.class);
    Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getResponseCode());
    Assert.assertTrue(response.getResponseBodyAsString(),
        response.getResponseBodyAsString().contains(Constants.Dataset.CUSTOM_MODULE_ENABLED));

    // nothing was registered
    Assert.assertTrue(getModules().getResponseObject().isEmpty());
    Assert.assertNull(dsFramework.getTypeInfo(NamespaceId.DEFAULT.datasetType("datasetType1")));

    // deleting all modules is what namespace deletion calls. It must keep working.
    Assert.assertEquals(HttpStatus.SC_OK, deleteModules().getResponseCode());
  }

  @Test
  public void testAdminOpsRejectModuleJar() throws Exception {
    // The jar does not exist. If the op executor tried to load it, the response would be a 500.
    File moduleJar = new File(TMP_FOLDER.newFolder(), "custom-module.jar");
    DatasetTypeMeta typeMeta = new DatasetTypeMeta(CUSTOM_TYPE, Collections.singletonList(
        new DatasetModuleMeta("customModule", "org.example.CustomModule", moduleJar.toURI(),
            Collections.singletonList(CUSTOM_TYPE), Collections.emptyList())));

    JsonObject create = new JsonObject();
    create.add("typeMeta", GSON.toJsonTree(typeMeta));
    create.add("instanceProps", GSON.toJsonTree(DatasetProperties.EMPTY));
    assertForbidden(executeAdminOp("custom", "create", create));

    DatasetSpecification spec = DatasetSpecification.builder("custom", CUSTOM_TYPE).build();
    JsonObject update = new JsonObject();
    update.add("typeMeta", GSON.toJsonTree(typeMeta));
    update.add("instanceProps", GSON.toJsonTree(DatasetProperties.EMPTY));
    update.add("existingSpec", GSON.toJsonTree(spec));
    assertForbidden(executeAdminOp("custom", "update", update));

    JsonObject drop = new JsonObject();
    drop.add("typeMeta", GSON.toJsonTree(typeMeta));
    drop.add("instanceSpec", GSON.toJsonTree(spec));
    assertForbidden(executeAdminOp("custom", "drop", drop));
  }

  @Test
  public void testSystemTypeIsNotAffected() throws Exception {
    // system dataset modules are on the classpath, so their type meta has no jar location
    DatasetTypeMeta typeMeta = dsFramework.getTypeInfo(NamespaceId.SYSTEM.datasetType("table"));
    Assert.assertNotNull(typeMeta);
    for (DatasetModuleMeta moduleMeta : typeMeta.getModules()) {
      Assert.assertNull(moduleMeta.getJarLocationPath());
    }

    JsonObject create = new JsonObject();
    create.add("typeMeta", GSON.toJsonTree(typeMeta));
    create.add("instanceProps", GSON.toJsonTree(DatasetProperties.EMPTY));
    HttpResponse response = executeAdminOp("systemTable", "create", create);
    Assert.assertEquals(response.getResponseBodyAsString(),
        HttpStatus.SC_OK, response.getResponseCode());
    JsonElement spec = GSON.fromJson(response.getResponseBodyAsString(), JsonObject.class)
        .get("spec");
    Assert.assertNotNull(spec);

    JsonObject drop = new JsonObject();
    drop.add("typeMeta", GSON.toJsonTree(typeMeta));
    drop.add("instanceSpec", spec);
    response = executeAdminOp("systemTable", "drop", drop);
    Assert.assertEquals(response.getResponseBodyAsString(),
        HttpStatus.SC_OK, response.getResponseCode());
  }

  private void assertForbidden(HttpResponse response) {
    Assert.assertEquals(response.getResponseBodyAsString(),
        HttpStatus.SC_FORBIDDEN, response.getResponseCode());
    Assert.assertTrue(response.getResponseBodyAsString(),
        response.getResponseBodyAsString().contains(Constants.Dataset.CUSTOM_MODULE_ENABLED));
  }

  /**
   * Sends an admin operation to the dataset op executor, in the same way the dataset service does.
   */
  private HttpResponse executeAdminOp(String instanceName, String opName, JsonObject body)
      throws IOException {
    DiscoveryServiceClient discoveryClient = injector.getInstance(DiscoveryServiceClient.class);
    Discoverable discoverable = new RandomEndpointStrategy(
        () -> discoveryClient.discover(Constants.Service.DATASET_EXECUTOR))
        .pick(10, TimeUnit.SECONDS);
    Assert.assertNotNull(discoverable);
    URL url = URIScheme.createURI(discoverable, "%s/namespaces/%s/data/datasets/%s/admin/%s",
        Constants.Gateway.API_VERSION_3_TOKEN, NamespaceId.DEFAULT.getNamespace(),
        instanceName, opName).toURL();
    return HttpRequests.execute(HttpRequest.post(url).withBody(GSON.toJson(body)).build(),
        REQUEST_CONFIG);
  }
}
