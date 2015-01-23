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
import co.cask.cdap.AppWithMultipleWorkflows;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.AdapterSpecification;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.Sink;
import co.cask.cdap.proto.Source;
import co.cask.cdap.test.internal.AppFabricClient;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
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
 * AdapterService life cycle tests.
 */
public class AdapterLifecycleTests extends AppFabricTestBase {
  private static final Gson GSON = new Gson();
  private static final Type ADAPTER_SPEC_LIST_TYPE = new TypeToken<List<AdapterSpecification>>() { }.getType();
  private static LocationFactory locationFactory;
  private static File adapterDir;
  private static AdapterService adapterService;

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration conf = getInjector().getInstance(CConfiguration.class);
    locationFactory = getInjector().getInstance(LocationFactory.class);
    adapterDir = new File(conf.get(Constants.AppFabric.ADAPTER_DIR));
    setupAdapters();
    adapterService = getInjector().getInstance(AdapterService.class);
    adapterService.registerAdapters();
  }

  @Test
  public void testAdapterLifeCycle() throws Exception {
    String namespaceId = Constants.DEFAULT_NAMESPACE;
    String adapterType = "dummyAdapter";
    String adapterName = "myStreamConverter";

    ImmutableMap<String, String> properties = ImmutableMap.of("frequency", "1m");
    ImmutableMap<String, String> sourceProperties = ImmutableMap.of();
    ImmutableMap<String, String> sinkProperties = ImmutableMap.of("dataset.class", FileSet.class.getName());

    AdapterSpecification specification =
      new AdapterSpecification(adapterName, adapterType, properties,
                               ImmutableSet.of(new Source("mySource", Source.Type.STREAM, sourceProperties)),
                               ImmutableSet.of(new Sink("mySink", Sink.Type.DATASET, sinkProperties)));

    HttpResponse response = createAdapter(namespaceId, adapterType, adapterName, "mySource", "mySink", properties,
                                          sourceProperties, sinkProperties);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    // A duplicate create request (or any other create request with the same namespace + adapterName) will result in 409
    response = createAdapter(namespaceId, adapterType, adapterName, "mySource", "mySink", properties,
                             sourceProperties, sinkProperties);
    //todo: change this to 409
    Assert.assertEquals(500, response.getStatusLine().getStatusCode());

    response = listAdapters(namespaceId);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    List<AdapterSpecification> list = readResponse(response, ADAPTER_SPEC_LIST_TYPE);
    Assert.assertEquals(1, list.size());
    Assert.assertEquals(specification, list.get(0));

    response = getAdapter(namespaceId, adapterName);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    AdapterSpecification receivedAdapterSpecification = readResponse(response, AdapterSpecification.class);
    Assert.assertEquals(specification, receivedAdapterSpecification);

    List<JsonObject> deployedApps = getAppList(namespaceId);
    Assert.assertEquals(1, deployedApps.size());
    JsonObject deployedApp = deployedApps.get(0);
    Assert.assertEquals(adapterType, deployedApp.get("id").getAsString());

    response = getAdapterStatus(namespaceId, adapterName);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String status = readResponse(response, String.class);
    Assert.assertEquals("STARTED", status);

    response = startStopAdapter(namespaceId, adapterName, "stop");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = getAdapterStatus(namespaceId, adapterName);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    status = readResponse(response, String.class);
    Assert.assertEquals("STOPPED", status);

    response = startStopAdapter(namespaceId, adapterName, "stop");
    Assert.assertEquals(409, response.getStatusLine().getStatusCode());

    response = startStopAdapter(namespaceId, adapterName, "start");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = getAdapterStatus(namespaceId, adapterName);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    status = readResponse(response, String.class);
    Assert.assertEquals("STARTED", status);

    response = deleteAdapter(namespaceId, adapterName);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = getAdapter(namespaceId, adapterName);
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testInvalidAdapters() throws Exception {
    //Invalid adapter  tests.
    String namespaceId = Constants.DEFAULT_NAMESPACE;

    ImmutableMap<String, String> properties = ImmutableMap.of("frequency", "1m");
    ImmutableMap<String, String> sourceProperties = ImmutableMap.of();
    ImmutableMap<String, String> sinkProperties = ImmutableMap.of("dataset.class", FileSet.class.getName());

    String adapterName = "myAdapter";
    String adapterType = "dummyAdapter";

    // Create Adapter without specifying the dataset.class attribute in the sink properties results in an error.
    HttpResponse httpResponse = createAdapter(namespaceId, adapterType, adapterName, "mySource", "mySink",
                                              properties, sourceProperties, ImmutableMap.<String, String>of());
    Assert.assertEquals(400, httpResponse.getStatusLine().getStatusCode());
    Assert.assertEquals("Dataset class cannot be null", EntityUtils.toString(httpResponse.getEntity()));

    // Create Adapter without specifying the frequency attribute in the adapter properties results in an error.
    httpResponse = createAdapter(namespaceId, adapterType, adapterName, "mySource", "mySink",
                                 ImmutableMap.<String, String>of(), sourceProperties, sinkProperties);
    Assert.assertEquals(400, httpResponse.getStatusLine().getStatusCode());
    Assert.assertEquals("Frequency of running the adapter is missing from adapter properties. Cannot schedule program.",
                        EntityUtils.toString(httpResponse.getEntity()));
  }

  private static void setupAdapters() throws IOException {
    setupAdapter(AdapterApp.class, "dummyAdapter");
    setupAdapter(AppWithMultipleWorkflows.class, "AppWithMultipleWorkflows");
  }

  private static void setupAdapter(Class<?> clz, String adapterType) throws IOException {
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
    return doPost(String.format("%s/namespaces/%s/adapters/%s",
                                Constants.Gateway.API_VERSION_3, namespaceId, name), adapterConfig);
  }

  private HttpResponse startStopAdapter(String namespaceId, String name, String action) throws Exception {
    return doPost(String.format("%s/namespaces/%s/adapters/%s/%s",
                                Constants.Gateway.API_VERSION_3, namespaceId, name, action));
  }

  private HttpResponse getAdapterStatus(String namespaceId, String name) throws Exception {
    return doGet(String.format("%s/namespaces/%s/adapters/%s/status",
                               Constants.Gateway.API_VERSION_3, namespaceId, name));
  }

  private HttpResponse listAdapters(String namespaceId) throws Exception {
    return doGet(String.format("%s/namespaces/%s/adapters",
                               Constants.Gateway.API_VERSION_3, namespaceId));
  }

  private HttpResponse getAdapter(String namespaceId, String adapterId) throws Exception {
    return doGet(String.format("%s/namespaces/%s/adapters/%s",
                               Constants.Gateway.API_VERSION_3, namespaceId, adapterId));
  }

  private HttpResponse startStopAdapter(String namespaceId, String adapterId, String action) throws Exception {
    return doPost(String.format("%s/namespaces/%s/adapters/%s/%s",
                                Constants.Gateway.API_VERSION_3, namespaceId, adapterId, action));
  }

  private HttpResponse deleteAdapter(String namespaceId, String adapterId) throws Exception {
    return doDelete(String.format("%s/namespaces/%s/adapters/%s",
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
