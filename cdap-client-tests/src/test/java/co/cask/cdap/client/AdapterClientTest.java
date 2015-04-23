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

import co.cask.cdap.api.templates.AdapterSpecification;
import co.cask.cdap.client.app.TemplateApp;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.AdapterNotFoundException;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.standalone.StandaloneTestBase;
import com.google.common.io.Files;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link AdapterClient}.
 * This can not be a apart of ClientTestsSuite, because it needs to do some setup before cdap is started. All test cases
 * in ClientTestsSuite share the same CDAP instance, and so may not have an opportunity to perform a setup step before
 * CDAP startup.
 */
@Category(XSlowTests.class)
public class AdapterClientTest extends ClientTestBase {

  private AdapterClient adapterClient;

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
  }

  @Test
  public void testAdapters() throws Exception {
    List<AdapterSpecification> initialList = adapterClient.list();
    Assert.assertEquals(0, initialList.size());

    String adapterName = "someAdapter";
    AdapterConfig adapterConfig = new AdapterConfig("description", TemplateApp.NAME, null);

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
    adapterClient.delete(adapterName);

    // verify that the adapter is deleted
    Assert.assertFalse(adapterClient.exists(adapterName));
    try {
      adapterClient.get(adapterName);
      Assert.fail();
    } catch (AdapterNotFoundException e) {
      // Expected
    }

    List<AdapterSpecification> finalList = adapterClient.list();
    Assert.assertEquals(0, finalList.size());
  }

  private static void setupAdapters(File adapterDir) throws IOException {
    setupAdapter(adapterDir, TemplateApp.class);
  }

  private static void setupAdapter(File adapterDir, Class<?> clz) throws IOException {
    File tempDir = TMP_FOLDER.newFolder();
    try {
      Location adapterJar = AppJarHelper.createDeploymentJar(new LocalLocationFactory(tempDir), clz);
      File destination =  new File(String.format("%s/%s", adapterDir.getAbsolutePath(), adapterJar.getName()));
      Files.copy(Locations.newInputSupplier(adapterJar), destination);
    } finally {
      DirUtils.deleteDirectoryContents(tempDir);
    }
  }
}
