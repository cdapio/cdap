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

import co.cask.cdap.DataTemplate;
import co.cask.cdap.DummyTemplate;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.AdapterNotFoundException;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.StreamDetail;
import co.cask.cdap.templates.AdapterSpecification;
import co.cask.cdap.test.internal.AppFabricClient;
import com.google.common.io.Files;
import org.apache.http.HttpResponse;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
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

  @Test(expected = RuntimeException.class)
  public void testInvalidTemplate() throws Exception {
    AdapterConfig adapterConfig = new AdapterConfig("description", BadTemplate.NAME, null);
    adapterService.createAdapter(Id.Namespace.from(TEST_NAMESPACE1), "badAdapter", adapterConfig);
  }

  @Test(expected = RuntimeException.class)
  public void testInvalidAdapter() throws Exception {
    Id.Namespace namespace = Id.Namespace.from(TEST_NAMESPACE1);
    String adapterName = "myAdapter";
    // the template should check that the first field is not null.
    DummyTemplate.Config config = new DummyTemplate.Config(null, "0 0 1 1 *");
    AdapterConfig adapterConfig = new AdapterConfig("description", DummyTemplate.NAME, GSON.toJsonTree(config));

    // Create Adapter
    adapterService.createAdapter(namespace, adapterName, adapterConfig);
  }

  @Test
  public void testAdapters() throws Exception {
    String adapterName = "myAdapter";
    DummyTemplate.Config config = new DummyTemplate.Config("somestream", "0 0 1 1 *");
    AdapterConfig adapterConfig = new AdapterConfig("description", DummyTemplate.NAME, GSON.toJsonTree(config));

    // Create Adapter
    adapterService.createAdapter(NAMESPACE, adapterName, adapterConfig);
    PreferencesStore preferencesStore = getInjector().getInstance(PreferencesStore.class);
    Map<String, String> prop = preferencesStore.getResolvedProperties(
      TEST_NAMESPACE1, adapterConfig.getTemplate());
    Assert.assertTrue(Boolean.parseBoolean(prop.get(ProgramOptionConstants.CONCURRENT_RUNS_ENABLED)));
    try {
      // Expect another call to create Adapter with the same adapterName to throw an AdapterAlreadyExistsException.
      adapterService.createAdapter(NAMESPACE, adapterName, adapterConfig);
      Assert.fail("Second call to create adapter with same adapterName did not throw AdapterAlreadyExistsException.");
    } catch (AdapterAlreadyExistsException expected) {
      // expected
    }

    AdapterSpecification actualAdapterSpec = adapterService.getAdapter(NAMESPACE, adapterName);
    Assert.assertNotNull(actualAdapterSpec);
    assertDummyConfigEquals(adapterConfig, actualAdapterSpec);

    // list all adapters
    Collection<AdapterSpecification> adapters = adapterService.getAdapters(NAMESPACE, DummyTemplate.NAME);
    Assert.assertEquals(1, adapters.size());
    AdapterSpecification actual = adapters.iterator().next();
    assertDummyConfigEquals(adapterConfig, actual);

    // adapter should be stopped
    Assert.assertEquals(AdapterStatus.STOPPED, adapterService.getAdapterStatus(NAMESPACE, adapterName));

    // start adapter
    adapterService.startAdapter(NAMESPACE, adapterName);
    Assert.assertEquals(AdapterStatus.STARTED, adapterService.getAdapterStatus(NAMESPACE, adapterName));

    // stop adapter
    adapterService.stopAdapter(NAMESPACE, adapterName);
    Assert.assertEquals(AdapterStatus.STOPPED, adapterService.getAdapterStatus(NAMESPACE, adapterName));

    // Delete Adapter
    adapterService.removeAdapter(NAMESPACE, adapterName);
    // verify that the adapter is deleted
    try {
      adapterService.getAdapter(NAMESPACE, adapterName);
      Assert.fail(String.format("Found adapterSpec with name %s; it should be deleted.", adapterName));
    } catch (AdapterNotFoundException expected) {
      // expected
    }

    adapters = adapterService.getAdapters(NAMESPACE, DummyTemplate.NAME);
    Assert.assertTrue(adapters.isEmpty());
  }

  @Test
  public void testRedeploy() throws Exception {
    ApplicationTemplateInfo info1 = adapterService.getApplicationTemplateInfo(DummyTemplate1.NAME);
    adapterService.deployTemplate(NAMESPACE, DummyTemplate1.NAME);
    ApplicationTemplateInfo info2 = adapterService.getApplicationTemplateInfo(DummyTemplate1.NAME);
    Assert.assertNotEquals(info1.getDescription(), info2.getDescription());
  }

  @Test
  public void testDataCreation() throws Exception {
    String streamName = "somestream";
    String tableName = "sometable";
    String adapterName = "streamAdapter";
    DataTemplate.Config config = new DataTemplate.Config(streamName, tableName);
    AdapterConfig adapterConfig = new AdapterConfig("description", DataTemplate.NAME, GSON.toJsonTree(config));
    adapterService.createAdapter(NAMESPACE, adapterName, adapterConfig);

    Assert.assertTrue(streamExists(Id.Stream.from(NAMESPACE, streamName)));
    Assert.assertTrue(datasetExists(Id.DatasetInstance.from(NAMESPACE, tableName)));
  }

  private void assertDummyConfigEquals(AdapterConfig expected, AdapterSpecification actual) {
    Assert.assertEquals(expected.getDescription(), actual.getDescription());
    Assert.assertEquals(expected.getTemplate(), actual.getTemplate());
    Assert.assertEquals(expected.getConfig(), actual.getConfig());
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
    setupAdapter(BadTemplate.class);
    setupAdapter(DataTemplate.class);
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

  /**
   * Bad template that contains 2 workflows.
   */
  public static class BadTemplate extends ApplicationTemplate {
    public static final String NAME = "badtemplate";

    @Override
    public void configure() {
      setName(NAME);
      addWorkflow(new SomeWorkflow1());
      addWorkflow(new SomeWorkflow2());
    }

    public static class SomeWorkflow1 extends AbstractWorkflow {
      @Override
      protected void configure() {
        setName("wf1");
        addMapReduce("DummyMapReduceJob");
      }
    }

    public static class SomeWorkflow2 extends AbstractWorkflow {
      @Override
      protected void configure() {
        setName("wf2");
        addMapReduce("DummyMapReduceJob");
      }
    }

    public static class DummyMapReduceJob extends AbstractMapReduce {
      @Override
      protected void configure() {
        setName("DummyMapReduceJob");
      }
    }
  }
}
