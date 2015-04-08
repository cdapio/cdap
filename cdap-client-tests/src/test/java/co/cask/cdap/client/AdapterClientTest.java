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

package co.cask.cdap.


  client;

import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.client.app.TemplateApp;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.AdapterNotFoundException;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.AdapterDetail;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.internal.AppFabricClient;
import co.cask.cdap.test.standalone.StandaloneTestBase;
import com.google.common.io.Files;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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

  private AdapterClient adapterClient;
  private ApplicationClient applicationClient;

  @BeforeClass
  public static void setUpClass() throws Exception {
    if (START_LOCAL_STANDALONE) {
      File adapterDir = TMP_FOLDER.newFolder("adapter");
      configuration = CConfiguration.create();
      configuration.set(Constants.AppFabric.APP_TEMPLATE_DIR, adapterDir.getAbsolutePath());
      setupAdapters(adapterDir);

      StandaloneTestBase.setUpClass();
    }
  }

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    clientConfig.setNamespace(Constants.DEFAULT_NAMESPACE_ID);
    adapterClient = new AdapterClient(clientConfig);
    applicationClient = new ApplicationClient(clientConfig);
  }

  @Test
  public void testAdapters() throws Exception {
    List<AdapterDetail> initialList = adapterClient.list();
    Assert.assertEquals(0, initialList.size());

    String adapterName = "someAdapter";
    AdapterConfig adapterConfig = new AdapterConfig("description", TemplateApp.NAME, null);

    // Create Adapter
    adapterClient.create("someAdapter", adapterConfig);

    // Check that the created adapter is present
    adapterClient.waitForExists("someAdapter", 30, TimeUnit.SECONDS);
    Assert.assertTrue(adapterClient.exists("someAdapter"));
    AdapterDetail someAdapter = adapterClient.get("someAdapter");
    Assert.assertNotNull(someAdapter);

    // list all adapters
    List<AdapterDetail> list = adapterClient.list();
    Assert.assertArrayEquals(new AdapterDetail[] {someAdapter}, list.toArray());

    // Delete Adapter
    adapterClient.delete(adapterName);

    // verify that the adapter is deleted
    Assert.assertFalse(adapterClient.exists(adapterName));
    try {
      adapterClient.get(adapterName);
      Assert.fail();
    } catch (AdapterNotFoundException e) {
      // Expected
    }

    List<AdapterDetail> finalList = adapterClient.list();
    Assert.assertEquals(0, finalList.size());
  }

  private static void setupAdapters(File adapterDir) throws IOException {
    setupAdapter(adapterDir, TemplateApp.class);
  }

  private static void setupAdapter(File adapterDir, Class<?> clz) throws IOException {

    Attributes attributes = new Attributes();
    attributes.put(ManifestFields.MAIN_CLASS, clz.getName());
    attributes.put(ManifestFields.MANIFEST_VERSION, "1.0");

    Manifest manifest = new Manifest();
    manifest.getMainAttributes().putAll(attributes);

    File tempDir = TMP_FOLDER.newFolder();
    try {
      File adapterJar = AppFabricClient.createDeploymentJar(new LocalLocationFactory(tempDir), clz, manifest);
      File destination =  new File(String.format("%s/%s", adapterDir.getAbsolutePath(), adapterJar.getName()));
      Files.copy(adapterJar, destination);
    } finally {
      DirUtils.deleteDirectoryContents(tempDir);
    }
  }
}
