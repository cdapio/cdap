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
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.internal.AppFabricClient;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Injector;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * AdapterService life cycle tests.
 */
public class AdapterServiceTests extends AppFabricTestBase {
  private static final Gson GSON = new Gson();
  private static final Type ADAPTER_SPEC_LIST_TYPE = new TypeToken<List<AdapterSpecification>>() { }.getType();
  private static LocationFactory locationFactory;
  private static File adapterDir;
  private static AdapterService adapterService;
  private static Store store;

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration conf = getInjector().getInstance(CConfiguration.class);
    Injector injector = getInjector();
    locationFactory = getInjector().getInstance(LocationFactory.class);
    adapterDir = new File(conf.get(Constants.AppFabric.ADAPTER_DIR));
    setupAdapters();
    adapterService = getInjector().getInstance(AdapterService.class);
    adapterService.registerAdapters();
    store = getInjector().getInstance(StoreFactory.class).create();
  }

  @Test
  public void testAdapters() throws Exception {

    //Basic adapter service tests.
    String namespaceId = Constants.DEFAULT_NAMESPACE;

    ImmutableMap<String, String> properties = ImmutableMap.of("frequency", "1m");
    ImmutableMap<String, String> sourceProperties = ImmutableMap.of();
    ImmutableMap<String, String> sinkProperties = ImmutableMap.of("dataset.class", FileSet.class.getName());

    String adapterName = "myAdapter";
    AdapterSpecification specification =
      new AdapterSpecification(adapterName, "dummyAdapter", properties,
                               ImmutableSet.of(new Source("mySource", Source.Type.STREAM, sourceProperties)),
                               ImmutableSet.of(new Sink("mySink", Sink.Type.DATASET, sinkProperties)));

    // Create Adapter
    // Note: AdapterService assumes the adapter is already deployed. So adding the application spec for the adapter
    AppFabricTestBase.deploy(AdapterApp.class, "dummyAdapter");
    adapterService.createAdapter(namespaceId, specification);

    AdapterSpecification retreivedAdapterSpec = adapterService.getAdapter(namespaceId, adapterName);
    Assert.assertNotNull(retreivedAdapterSpec);
    Assert.assertEquals(specification, retreivedAdapterSpec);
  }

  private static void setupAdapters() throws IOException {
    setupAdapter(AdapterApp.class, "dummyAdapter");
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
}
