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

import co.cask.cdap.DummyTemplate;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.AdapterNotFoundException;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.internal.AppFabricClient;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.gson.reflect.TypeToken;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * AdapterService life cycle tests.
 */
public class AdapterServiceTests extends AppFabricTestBase {
  private static LocationFactory locationFactory;
  private static File adapterDir;
  private static AdapterService adapterService;

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration conf = getInjector().getInstance(CConfiguration.class);
    locationFactory = getInjector().getInstance(LocationFactory.class);
    adapterDir = new File(conf.get(Constants.AppFabric.APP_TEMPLATE_DIR));
    setupAdapters();
    adapterService = getInjector().getInstance(AdapterService.class);
    adapterService.registerTemplates();
  }

  @Test
  public void testAdapters() throws Exception {
    String adapterName = "myAdapter";
    DummyTemplate.Config config = new DummyTemplate.Config("* * * * *", "myStream");

    AdapterConfig<DummyTemplate.Config> adapterConfig =
      new AdapterConfig<DummyTemplate.Config>(adapterName, "", DummyTemplate.NAME, config);

    Id.Namespace namespace = Id.Namespace.from(TEST_NAMESPACE1);
    // Create Adapter
    adapterService.createAdapter(namespace, adapterConfig);
    PreferencesStore preferencesStore = getInjector().getInstance(PreferencesStore.class);
    Map<String, String> prop = preferencesStore.getResolvedProperties(
      TEST_NAMESPACE1, adapterConfig.getTemplate());
    Assert.assertTrue(Boolean.parseBoolean(prop.get(ProgramOptionConstants.CONCURRENT_RUNS_ENABLED)));
    try {
      // Expect another call to create Adapter with the same adapterName to throw an AdapterAlreadyExistsException.
      adapterService.createAdapter(namespace, adapterConfig);
      Assert.fail("Second call to create adapter with same adapterName did not throw AdapterAlreadyExistsException.");
    } catch (AdapterAlreadyExistsException expected) {
      // expected
    }

    AdapterConfig<DummyTemplate.Config> actualAdapterSpec =
      adapterService.getAdapter(namespace, adapterName, DummyTemplate.Config.class);
    Assert.assertNotNull(actualAdapterSpec);
    Assert.assertEquals(adapterConfig, actualAdapterSpec);

    // list all adapters
    Collection<AdapterConfig<Object>> adapters = adapterService.getAdapters(namespace, DummyTemplate.NAME);
    Assert.assertEquals(1, adapters.size());
    AdapterConfig<Object> actualRaw = adapters.iterator().next();
    AdapterConfig<DummyTemplate.Config> actual = GSON.fromJson(
      GSON.toJson(actualRaw), new TypeToken<AdapterConfig<DummyTemplate.Config>>() { }.getType());
    Assert.assertEquals(actual, adapterConfig);

    // Delete Adapter
    adapterService.removeAdapter(namespace, "myAdapter");
    // verify that the adapter is deleted
    try {
      adapterService.getAdapter(namespace, adapterName, Object.class);
      Assert.fail(String.format("Found adapterSpec with name %s; it should be deleted.", adapterName));
    } catch (AdapterNotFoundException expected) {
      // expected
    }

    adapters = adapterService.getAdapters(namespace, DummyTemplate.NAME);
    Assert.assertTrue(adapters.isEmpty());
  }

  @Test
  public void testGetAllAdapters() throws Exception {
    Id.Namespace namespace = Id.Namespace.from(TEST_NAMESPACE1);

    AdapterConfig<int[]> spec1 =
      new AdapterConfig<int[]>("adapter1", "desc1", "template1", new int[] { 1, 2, 3 });
    AdapterConfig<Map<String, String>> spec2 =
      new AdapterConfig<Map<String, String>>("adapter2", "desc2", "template2", ImmutableMap.of("k1", "v1"));

    // Create Adapters
    adapterService.createAdapter(namespace, spec1);
    adapterService.createAdapter(namespace, spec2);

    // check get all
    Collection<AdapterConfig<Object>> adapters = adapterService.getAdapters(namespace);
    Assert.assertEquals(2, adapters.size());
    Iterator<AdapterConfig<Object>> iter = adapters.iterator();
    AdapterConfig<Object> actual1 = iter.next();
    AdapterConfig<Object> actual2 = iter.next();
    if (actual1.getName().equals(spec1.getName())) {
      assertArrayConfigEquals(spec1, actual1);
      assertMapConfigEquals(spec2, actual2);
    } else {
      assertArrayConfigEquals(spec1, actual2);
      assertMapConfigEquals(spec2, actual1);
    }

    // check get all for a specific template
    Collection<AdapterConfig<Object>> template1Adapters =
      adapterService.getAdapters(namespace, spec1.getTemplate());
    Assert.assertEquals(1, template1Adapters.size());
    assertArrayConfigEquals(spec1, template1Adapters.iterator().next());

    Collection<AdapterConfig<Object>> template2Adapters =
      adapterService.getAdapters(namespace, spec2.getTemplate());
    Assert.assertEquals(1, template2Adapters.size());
    assertMapConfigEquals(spec2, template2Adapters.iterator().next());
  }

  private void assertArrayConfigEquals(AdapterConfig<int[]> expected, AdapterConfig<Object> actual) {
    Assert.assertEquals(expected.getName(), actual.getName());
    Assert.assertEquals(expected.getDescription(), actual.getDescription());
    Assert.assertEquals(expected.getTemplate(), actual.getTemplate());
    Assert.assertArrayEquals(expected.getConfig(), GSON.fromJson(GSON.toJson(actual.getConfig()), int[].class));
  }

  private void assertMapConfigEquals(AdapterConfig<Map<String, String>> expected,
                                     AdapterConfig<Object> actual) {
    Type type = new TypeToken<AdapterConfig<Map<String, String>>>() { }.getType();
    AdapterConfig<Map<String, String>> actualAsMap = GSON.fromJson(GSON.toJson(actual), type);
    Assert.assertEquals(expected, actualAsMap);
  }

  @Test
  public void testInvalidJars() throws Exception {
    Class<?> clz = DummyTemplate.class;
    String adapterType = "adapterType";

    Attributes attributes = generateRequiredAttributes(clz, adapterType);
    setupAdapterJarWithManifestAttributes(clz, attributes);

    // Using a valid manifest (no missing attributes) results in the adapterTypeInfo being registered
    adapterService.registerTemplates();
    Assert.assertNotNull(adapterService.getApplicationTemplateInfo(adapterType));

    // removing the any of the required attributes from the manifest results in the AdapterTypeInfo not being created.
    // Missing the CDAP-Source-Type attribute
    adapterType = "adapterType1";
    attributes = new Attributes();
    attributes.putValue("CDAP-Sink-Type", "DATASET");
    attributes.putValue("CDAP-Adapter-Type", adapterType);
    attributes.putValue("CDAP-Adapter-Program-Type", ProgramType.WORKFLOW.toString());
    setupAdapterJarWithManifestAttributes(clz, attributes);

    adapterService.registerTemplates();
    Assert.assertNull(adapterService.getApplicationTemplateInfo(adapterType));

    // Missing the CDAP-Sink-Type attribute
    adapterType = "adapterType2";
    attributes = new Attributes();
    attributes.putValue("CDAP-Source-Type", "STREAM");
    attributes.putValue("CDAP-Adapter-Type", adapterType);
    attributes.putValue("CDAP-Adapter-Program-Type", ProgramType.WORKFLOW.toString());
    setupAdapterJarWithManifestAttributes(clz, attributes);

    adapterService.registerTemplates();
    Assert.assertNull(adapterService.getApplicationTemplateInfo(adapterType));

    // Missing the CDAP-Adapter-Type attribute
    adapterType = "adapterType3";
    attributes = new Attributes();
    attributes.putValue("CDAP-Source-Type", "STREAM");
    attributes.putValue("CDAP-Sink-Type", "DATASET");
    attributes.putValue("CDAP-Adapter-Program-Type", ProgramType.WORKFLOW.toString());
    setupAdapterJarWithManifestAttributes(clz, attributes);

    adapterService.registerTemplates();
    Assert.assertNull(adapterService.getApplicationTemplateInfo(adapterType));

    // Missing the CDAP-Adapter-Program-Type attribute
    adapterType = "adapterType4";
    attributes = new Attributes();
    attributes.putValue("CDAP-Source-Type", "STREAM");
    attributes.putValue("CDAP-Sink-Type", "DATASET");
    attributes.putValue("CDAP-Adapter-Type", adapterType);
    setupAdapterJarWithManifestAttributes(clz, attributes);

    adapterService.registerTemplates();
    Assert.assertNull(adapterService.getApplicationTemplateInfo(adapterType));
  }

  private static Attributes generateRequiredAttributes(Class<?> clz, String adapterType) {
    Attributes attributes = new Attributes();
    attributes.put(ManifestFields.MAIN_CLASS, clz.getName());
    attributes.put(ManifestFields.MANIFEST_VERSION, "1.0");
    attributes.putValue("CDAP-Source-Type", "STREAM");
    attributes.putValue("CDAP-Sink-Type", "DATASET");
    attributes.putValue("CDAP-Adapter-Type", adapterType);
    attributes.putValue("CDAP-Adapter-Program-Type", ProgramType.WORKFLOW.toString());
    return attributes;
  }


  private void setupAdapterJarWithManifestAttributes(Class<?> clz, Attributes attributes) throws IOException {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().putAll(attributes);
    File adapterJar = AppFabricClient.createDeploymentJar(locationFactory, clz, manifest);
    File destination =  new File(String.format("%s/%s", adapterDir.getAbsolutePath(), adapterJar.getName()));
    Files.copy(adapterJar, destination);
  }

  private static void setupAdapters() throws IOException {
    setupAdapter(DummyTemplate.class, DummyTemplate.NAME);
    setupAdapter(DummyTemplate.class, "template1");
    setupAdapter(DummyTemplate.class, "template2");
  }

  private static void setupAdapter(Class<?> clz, String adapterType) throws IOException {
    Attributes attributes = generateRequiredAttributes(clz, adapterType);

    Manifest manifest = new Manifest();
    manifest.getMainAttributes().putAll(attributes);

    File adapterJar = AppFabricClient.createDeploymentJar(locationFactory, clz, manifest);
    File destination =  new File(String.format("%s/%s", adapterDir.getAbsolutePath(), adapterJar.getName()));
    Files.copy(adapterJar, destination);
  }
}
