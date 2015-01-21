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

package co.cask.cdap.client;

import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.client.app.AdapterApp;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.client.exception.AdapterNotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.AdapterSpecification;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.internal.AppFabricClient;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Test for {@link AdapterClient}.
 */
@Category(XSlowTests.class)
public class AdapterClientTestRun extends ClientTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(AdapterClientTestRun.class);

  private AdapterClient adapterClient;
  private File adapterDir;

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    adapterDir = new File(configuration.get(Constants.AppFabric.ADAPTER_DIR));
    adapterClient = new AdapterClient(clientConfig);
  }

  @Test
  @Ignore //https://issues.cask.co/browse/CDAP-1221
  public void testAdapters() throws Exception {
    String namespaceId = Constants.DEFAULT_NAMESPACE;
    setupAdapters();

    List<AdapterSpecification> initialList = adapterClient.list(namespaceId);
    Assert.assertEquals(0, initialList.size());

    AdapterConfig adapterConfig = new AdapterConfig();
    adapterConfig.type = "dummyAdapter";
    adapterConfig.properties = ImmutableMap.of("frequency", "1m");
    adapterConfig.source = new AdapterConfig.Source("mySource", ImmutableMap.<String, String>of());
    adapterConfig.sink = new AdapterConfig.Sink("mySink", ImmutableMap.of("dataset.class", FileSet.class.getName()));

    // Create Adapter
    adapterClient.create(namespaceId, "someAdapter", adapterConfig);

    // Check that the created adapter is present
    adapterClient.waitForExists(namespaceId, "someAdapter", 30, TimeUnit.SECONDS);
    Assert.assertTrue(adapterClient.exists(namespaceId, "someAdapter"));
    AdapterSpecification someAdapter = adapterClient.get(namespaceId, "someAdapter");
    Assert.assertNotNull(someAdapter);

    // list all adapters
    List<AdapterSpecification> list = adapterClient.list(namespaceId);
    Assert.assertArrayEquals(new AdapterSpecification[] {someAdapter}, list.toArray());

    // Delete Adapter
    adapterClient.delete(namespaceId, "someAdapter");

    // verify that the adapter is deleted
    Assert.assertFalse(adapterClient.exists(namespaceId, "someAdapter"));
    try {
      adapterClient.get(namespaceId, "someAdapter");
      Assert.fail();
    } catch (AdapterNotFoundException e) {
      // Expected
    }

    List<AdapterSpecification> finalList = adapterClient.list(namespaceId);
    Assert.assertEquals(0, finalList.size());
  }

  private void setupAdapters() throws IOException {
    setupAdapter(AdapterApp.class, "dummyAdapter");
  }

  private void setupAdapter(Class<?> clz, String adapterType) throws IOException {

    Attributes attributes = new Attributes();
    attributes.put(ManifestFields.MAIN_CLASS, clz.getName());
    attributes.put(ManifestFields.MANIFEST_VERSION, "1.0");
    attributes.putValue("CDAP-Source-Type", "STREAM");
    attributes.putValue("CDAP-Sink-Type", "DATASET");
    attributes.putValue("CDAP-Adapter-Type", adapterType);
    attributes.putValue("CDAP-Adapter-Program-Type", ProgramType.WORKFLOW.toString());

    Manifest manifest = new Manifest();
    manifest.getMainAttributes().putAll(attributes);

    File tempDir = Files.createTempDir();
    try {
      File adapterJar = AppFabricClient.createDeploymentJar(new LocalLocationFactory(tempDir), clz, manifest);
      File destination =  new File(String.format("%s/%s", adapterDir.getAbsolutePath(), adapterJar.getName()));
      Files.copy(adapterJar, destination);
    } finally {
      DirUtils.deleteDirectoryContents(tempDir);
    }
  }
}
