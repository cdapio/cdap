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
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.AdapterNotFoundException;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.AdapterSpecification;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.internal.AppFabricClient;
import co.cask.cdap.test.standalone.StandaloneTestBase;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
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
 * This can not be a apart of ClientTestsSuite, because it needs to do some setup before cdap is started. All test cases
 * in ClientTestsSuite share the same CDAP instance, and so may not have an opportunity to perform a setup step before
 * CDAP startup.
 */
@Category(XSlowTests.class)
public class AdapterClientTest extends ClientTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(AdapterClientTest.class);

  private AdapterClient adapterClient;
  private ApplicationClient applicationClient;

  @BeforeClass
  public static void setUpClass() throws Exception {
    if (START_LOCAL_STANDALONE) {
      File adapterDir = TMP_FOLDER.newFolder("adapter");
      configuration = CConfiguration.create();
      configuration.set(Constants.AppFabric.ADAPTER_DIR, adapterDir.getAbsolutePath());
      setupAdapters(adapterDir);

      StandaloneTestBase.setUpClass();
    }
  }

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    clientConfig.setNamespace(Constants.DEFAULT_NAMESPACE);
    adapterClient = new AdapterClient(clientConfig);
    applicationClient = new ApplicationClient(clientConfig);
  }

  @Test
  public void testAdapters() throws Exception {
    List<AdapterSpecification> initialList = adapterClient.list();
    Assert.assertEquals(0, initialList.size());

    AdapterConfig adapterConfig = new AdapterConfig();
    adapterConfig.type = "dummyAdapter";
    adapterConfig.properties = ImmutableMap.of("frequency", "1m");
    adapterConfig.source = new AdapterConfig.Source("mySource", ImmutableMap.<String, String>of());
    adapterConfig.sink = new AdapterConfig.Sink("mySink", ImmutableMap.of("dataset.class", FileSet.class.getName()));

    // Create Adapter
    adapterClient.create("someAdapter", adapterConfig);

    // Check that the created adapter is present
    adapterClient.waitForExists("someAdapter", 30, TimeUnit.SECONDS);
    Assert.assertTrue(adapterClient.exists("someAdapter"));
    AdapterSpecification someAdapter = adapterClient.get("someAdapter");
    Assert.assertNotNull(someAdapter);

    // list all adapters
    List<AdapterSpecification> list = adapterClient.list();
    Assert.assertArrayEquals(new AdapterSpecification[] {someAdapter}, list.toArray());

    // Delete Adapter
    adapterClient.delete("someAdapter");

    // verify that the adapter is deleted
    Assert.assertFalse(adapterClient.exists("someAdapter"));
    try {
      adapterClient.get("someAdapter");
      Assert.fail();
    } catch (AdapterNotFoundException e) {
      // Expected
    }

    List<AdapterSpecification> finalList = adapterClient.list();
    Assert.assertEquals(0, finalList.size());

    applicationClient.deleteAll();
    applicationClient.waitForDeleted("dummyAdapter", 30, TimeUnit.SECONDS);
  }

  private static void setupAdapters(File adapterDir) throws IOException {
    setupAdapter(adapterDir, AdapterApp.class, "dummyAdapter");
  }

  private static void setupAdapter(File adapterDir, Class<?> clz, String adapterType) throws IOException {

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
