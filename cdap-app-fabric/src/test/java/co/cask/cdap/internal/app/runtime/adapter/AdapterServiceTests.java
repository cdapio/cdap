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
import co.cask.cdap.proto.AdapterSpecification;
import co.cask.cdap.proto.Id;
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
import java.util.UUID;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * AdapterService life cycle tests.
 */
public class AdapterServiceTests extends AppFabricTestBase {
  private static final Id.Namespace NAMESPACE = Id.Namespace.from(TEST_NAMESPACE1);
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

    AdapterSpecification<DummyTemplate.Config> adapterSpecification =
      new AdapterSpecification<DummyTemplate.Config>(adapterName, "", DummyTemplate.NAME, config);

    // Create Adapter
    adapterService.createAdapter(NAMESPACE, adapterSpecification);
    PreferencesStore preferencesStore = getInjector().getInstance(PreferencesStore.class);
    Map<String, String> prop = preferencesStore.getResolvedProperties(
      NAMESPACE.getId(), adapterSpecification.getTemplate());
    Assert.assertTrue(Boolean.parseBoolean(prop.get(ProgramOptionConstants.CONCURRENT_RUNS_ENABLED)));
    try {
      // Expect another call to create Adapter with the same adapterName to throw an AdapterAlreadyExistsException.
      adapterService.createAdapter(NAMESPACE, adapterSpecification);
      Assert.fail("Second call to create adapter with same adapterName did not throw AdapterAlreadyExistsException.");
    } catch (AdapterAlreadyExistsException expected) {
      // expected
    }

    AdapterSpecification<DummyTemplate.Config> actualAdapterSpec =
      adapterService.getAdapter(NAMESPACE, adapterName, DummyTemplate.Config.class);
    Assert.assertNotNull(actualAdapterSpec);
    Assert.assertEquals(adapterSpecification, actualAdapterSpec);

    // list all adapters
    Collection<AdapterSpecification<Object>> adapters = adapterService.getAdapters(NAMESPACE, DummyTemplate.NAME);
    Assert.assertEquals(1, adapters.size());
    AdapterSpecification<Object> actualRaw = adapters.iterator().next();
    AdapterSpecification<DummyTemplate.Config> actual = GSON.fromJson(
      GSON.toJson(actualRaw), new TypeToken<AdapterSpecification<DummyTemplate.Config>>() {
    }.getType());
    Assert.assertEquals(actual, adapterSpecification);

    // Delete Adapter
    adapterService.removeAdapter(NAMESPACE, "myAdapter");
    // verify that the adapter is deleted
    try {
      adapterService.getAdapter(NAMESPACE, adapterName, Object.class);
      Assert.fail(String.format("Found adapterSpec with name %s; it should be deleted.", adapterName));
    } catch (AdapterNotFoundException expected) {
      // expected
    }

    adapters = adapterService.getAdapters(NAMESPACE, DummyTemplate.NAME);
    Assert.assertTrue(adapters.isEmpty());
  }

  @Test
  public void testGetAllAdapters() throws Exception {

    AdapterSpecification<int[]> spec1 =
      new AdapterSpecification<int[]>("adapter1", "desc1", DummyTemplate1.NAME, new int[] { 1, 2, 3 });
    AdapterSpecification<Map<String, String>> spec2 =
      new AdapterSpecification<Map<String, String>>("adapter2", "desc2",
                                                    DummyTemplate2.NAME, ImmutableMap.of("k1", "v1"));

    // Create Adapters
    adapterService.createAdapter(NAMESPACE, spec1);
    adapterService.createAdapter(NAMESPACE, spec2);

    // check get all
    Collection<AdapterSpecification<Object>> adapters = adapterService.getAdapters(NAMESPACE);
    Assert.assertEquals(2, adapters.size());
    Iterator<AdapterSpecification<Object>> iter = adapters.iterator();
    AdapterSpecification<Object> actual1 = iter.next();
    AdapterSpecification<Object> actual2 = iter.next();
    if (actual1.getName().equals(spec1.getName())) {
      assertArrayConfigEquals(spec1, actual1);
      assertMapConfigEquals(spec2, actual2);
    } else {
      assertArrayConfigEquals(spec1, actual2);
      assertMapConfigEquals(spec2, actual1);
    }

    // check get all for a specific template
    Collection<AdapterSpecification<Object>> template1Adapters =
      adapterService.getAdapters(NAMESPACE, spec1.getTemplate());
    Assert.assertEquals(1, template1Adapters.size());
    assertArrayConfigEquals(spec1, template1Adapters.iterator().next());

    Collection<AdapterSpecification<Object>> template2Adapters =
      adapterService.getAdapters(NAMESPACE, spec2.getTemplate());
    Assert.assertEquals(1, template2Adapters.size());
    assertMapConfigEquals(spec2, template2Adapters.iterator().next());
  }

  @Test
  public void testRedeploy() throws Exception {
    ApplicationTemplateInfo info1 = adapterService.getApplicationTemplateInfo(DummyTemplate1.NAME);
    adapterService.deployTemplate(NAMESPACE, DummyTemplate1.NAME);
    ApplicationTemplateInfo info2 = adapterService.getApplicationTemplateInfo(DummyTemplate1.NAME);
    Assert.assertNotEquals(info1.getDescription(), info2.getDescription());
  }

  private void assertArrayConfigEquals(AdapterSpecification<int[]> expected, AdapterSpecification<Object> actual) {
    Assert.assertEquals(expected.getName(), actual.getName());
    Assert.assertEquals(expected.getDescription(), actual.getDescription());
    Assert.assertEquals(expected.getTemplate(), actual.getTemplate());
    Assert.assertArrayEquals(expected.getConfig(), GSON.fromJson(GSON.toJson(actual.getConfig()), int[].class));
  }

  private void assertMapConfigEquals(AdapterSpecification<Map<String, String>> expected,
                                     AdapterSpecification<Object> actual) {
    Type type = new TypeToken<AdapterSpecification<Map<String, String>>>() { }.getType();
    AdapterSpecification<Map<String, String>> actualAsMap = GSON.fromJson(GSON.toJson(actual), type);
    Assert.assertEquals(expected, actualAsMap);
  }

  private static Attributes generateRequiredAttributes(Class<?> clz) {
    Attributes attributes = new Attributes();
    attributes.put(ManifestFields.MAIN_CLASS, clz.getName());
    attributes.put(ManifestFields.MANIFEST_VERSION, "1.0");
    return attributes;
  }

  private static void setupAdapters() throws IOException {
    setupAdapter(DummyTemplate.class);
    setupAdapter(DummyTemplate1.class);
    setupAdapter(DummyTemplate2.class);
  }

  private static void setupAdapter(Class<?> clz) throws IOException {
    Attributes attributes = generateRequiredAttributes(clz);

    Manifest manifest = new Manifest();
    manifest.getMainAttributes().putAll(attributes);

    File adapterJar = AppFabricClient.createDeploymentJar(locationFactory, clz, manifest);
    File destination =  new File(String.format("%s/%s", adapterDir.getAbsolutePath(), adapterJar.getName()));
    Files.copy(adapterJar, destination);
  }

  public static class DummyTemplate1 extends DummyTemplate {
    public static final String NAME = "template1";

    @Override
    public void configure() {
      super.configure();
      setName(NAME);
    }
  }

  public static class DummyTemplate2 extends DummyTemplate {
    public static final String NAME = "template2";
    @Override
    public void configure() {
      super.configure();
      setName(NAME);
    }
  }
}
