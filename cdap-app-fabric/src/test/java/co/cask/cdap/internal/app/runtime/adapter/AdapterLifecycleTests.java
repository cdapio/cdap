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

package co.cask.cdap.internal.app.runtime.adapter;

import co.cask.cdap.AdapterApp;
import co.cask.cdap.adapter.AdapterSpecification;
import co.cask.cdap.adapter.Sink;
import co.cask.cdap.adapter.Source;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.internal.AppFabricClient;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 *
 */
public class AdapterLifecycleTests extends AppFabricTestBase {
  private static final Gson GSON = new Gson();
  private static final String TEST_NAMESPACE1 = "testnamespace1";
  private static final String TEST_NAMESPACE2 = "testnamespace2";
  private static final NamespaceMeta TEST_NAMESPACE_META1 = new NamespaceMeta.Builder()
    .setDisplayName(TEST_NAMESPACE1).setDescription(TEST_NAMESPACE1).build();
  private static final NamespaceMeta TEST_NAMESPACE_META2 = new NamespaceMeta.Builder()
    .setDisplayName(TEST_NAMESPACE2).setDescription(TEST_NAMESPACE2).build();

  private static final Type ADAPTER_SPEC_LIST_TYPE = new TypeToken<List<AdapterSpecification>>() { }.getType();
  private static final Map<String, String> EMPTY_MAP = ImmutableMap.of();
  private static LocationFactory locationFactory;
  private static File adapterDir;
  private static AdapterService adapterService;

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration conf = getInjector().getInstance(CConfiguration.class);
    locationFactory = getInjector().getInstance(LocationFactory.class);
    adapterDir = new File(conf.get(Constants.AppFabric.ADAPTER_DIR));

    HttpResponse response = doPut(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, TEST_NAMESPACE1),
                                  GSON.toJson(TEST_NAMESPACE_META1));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doPut(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, TEST_NAMESPACE2),
                     GSON.toJson(TEST_NAMESPACE_META2));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    setupAdapters();
    adapterService = getInjector().getInstance(AdapterService.class);
    adapterService.registerAdapters();
  }
  @Test
  public void testAdapterLifeCycle() throws Exception {
    setupAdapters();

    String namespaceId = Constants.DEFAULT_NAMESPACE;
    String adapterId = "dummyAdapter";
    String adapterName = "myStreamConvertor";

    ImmutableMap<String, String> properties = ImmutableMap.of("frequency", "1m");
    ImmutableMap<String, String> sourceProperties = ImmutableMap.of();
    ImmutableMap<String, String> sinkProperties = ImmutableMap.of("dataset.class", FileSet.class.getName());

    AdapterSpecification specification =
      new AdapterSpecification(adapterName, adapterId, properties,
                               ImmutableSet.of(new Source("mySource", Source.Type.STREAM, sourceProperties)),
                               ImmutableSet.of(new Sink("mySink", Sink.Type.DATASET, sinkProperties)));

    HttpResponse response = createAdapter(namespaceId, adapterId, adapterName, "mySource", "mySink", properties,
                                          sourceProperties, sinkProperties);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = listAdapters(namespaceId);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    List<AdapterSpecification> list = readResponse(response, ADAPTER_SPEC_LIST_TYPE);
    Assert.assertEquals(1, list.size());
    Assert.assertEquals(specification, list.get(0));

    response = getAdapter(namespaceId, adapterName);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    AdapterSpecification receivedAdapterSpecification = readResponse(response, AdapterSpecification.class);
    Assert.assertEquals(specification, receivedAdapterSpecification);
    //TODO: Add Delete tests
  }

  @Test
  public void testNonexistentAdapter() throws Exception {
    String nonexistentAdapterId = "nonexistentAdapterId";
    HttpResponse response = getAdapter(Constants.DEFAULT_NAMESPACE, nonexistentAdapterId);
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
  }



  private static void setupAdapters() throws IOException {
    setupAdapter(AdapterApp.class, "dummyAdapter", "AdapterWorkflow");
  }

  private static void setupAdapter(Class<?> clz, String adapterType, String scheduledProgram) throws IOException {

    Attributes attributes = new Attributes();
    attributes.put(ManifestFields.MAIN_CLASS, clz.getName());
    attributes.put(ManifestFields.MANIFEST_VERSION, "1.0");
    attributes.putValue("CDAP-Source-Type", "STREAM");
    attributes.putValue("CDAP-Sink-Type", "DATASET");
    attributes.putValue("CDAP-Adapter-Type", adapterType);
    attributes.putValue("CDAP-Adapter-Program-Type", ProgramType.WORKFLOW.toString());

    Manifest manifest = new Manifest();
    manifest.getMainAttributes().putAll(attributes);

    File adapterJar = AppFabricClient.createDeploymentJar(locationFactory, clz, manifest);
    File destination =  new File(String.format("%s/%s", adapterDir.getAbsolutePath(), adapterJar.getName()));
    Files.copy(adapterJar, destination);
  }

  private HttpResponse createAdapter(String namespaceId, String type, String name, String sourceName,
                                     String sinkName, ImmutableMap<String, String> adapterProperties,
                                     ImmutableMap<String, String> sourceProperties,
                                     ImmutableMap<String, String> sinkProperties) throws Exception {
    JsonObject source = new JsonObject();
    source.addProperty("name", sourceName);
    source.add("properties", toJsonObject(sourceProperties));

    JsonObject sink = new JsonObject();
    sink.addProperty("name", sinkName);
    sink.add("properties", toJsonObject(sinkProperties));

    JsonObject adapterConfig = new JsonObject();
    adapterConfig.addProperty("type", type);
    adapterConfig.add("properties", toJsonObject(adapterProperties));
    adapterConfig.add("source", source);
    adapterConfig.add("sink", sink);

    return createAdapter(namespaceId, name, GSON.toJson(adapterConfig));
  }

  private HttpResponse createAdapter(String namespaceId, String name, String adapterConfig) throws Exception {
    return doPut(String.format("%s/namespaces/%s/adapters/%s",
                               Constants.Gateway.API_VERSION_3, namespaceId, name), adapterConfig);
  }

  private HttpResponse listAdapters(String namespaceId) throws Exception {
    return doGet(String.format("%s/namespaces/%s/adapters",
                               Constants.Gateway.API_VERSION_3, namespaceId));
  }

  private HttpResponse getAdapter(String namespaceId, String adapterId) throws Exception {
    return doGet(String.format("%s/namespaces/%s/adapters/%s",
                               Constants.Gateway.API_VERSION_3, namespaceId, adapterId));
  }

  private JsonObject toJsonObject(Map<String, String> properties) {
    JsonObject jsonProperties = new JsonObject();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      jsonProperties.addProperty(entry.getKey(), entry.getValue());
    }
    return jsonProperties;
  }
}
