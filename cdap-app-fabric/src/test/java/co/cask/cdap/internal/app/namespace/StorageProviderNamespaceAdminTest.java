/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.namespace;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.DefaultNamespacedLocationFactory;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.internal.guice.AppFabricTestModule;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.store.NamespaceStore;
import co.cask.tephra.TransactionManager;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import org.apache.twill.filesystem.Location;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

/**
 * Tests for {@link StorageProviderNamespaceAdmin}
 */
public class StorageProviderNamespaceAdminTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static NamespacedLocationFactory namespacedLocationFactory;
  private static StorageProviderNamespaceAdmin storageProviderNamespaceAdmin;
  private static NamespaceStore namespaceStore;
  private static TransactionManager transactionManager;
  private static DatasetService datasetService;

  @BeforeClass
  public static void setup() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setBoolean(Constants.Explore.EXPLORE_ENABLED, true);
    Injector injector = Guice.createInjector(Modules.override(new AppFabricTestModule(cConf)).with(
      new AbstractModule() {
        @Override
        protected void configure() {
          // use the DefaultNamespacedLocationFactory here to test proper namespace creation in storage handler and
          // not the NamespacedLocationFactoryTestClient
          bind(NamespacedLocationFactory.class).to(DefaultNamespacedLocationFactory.class);
        }
      }
    ));
    namespacedLocationFactory = injector.getInstance(NamespacedLocationFactory.class);
    storageProviderNamespaceAdmin = injector.getInstance(StorageProviderNamespaceAdmin.class);
    // start the dataset service for namespace store to work
    transactionManager = injector.getInstance(TransactionManager.class);
    transactionManager.startAndWait();
    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();
    // we don't use namespace admin here but the store because namespaceadmin will try to create the
    // home directory for namespace which we don't want. We just want to store the namespace meta in store
    // to look up during the delete.
    namespaceStore = injector.getInstance(NamespaceStore.class);
  }

  @Test
  public void test() throws Exception {
    NamespaceId myspace = new NamespaceId("myspace");
    NamespaceMeta myspaceMeta = new NamespaceMeta.Builder().setName(myspace.getNamespace()).build();
    // the create/delete will look up meta so store that too
    namespaceStore.create(myspaceMeta);
    storageProviderNamespaceAdmin.create(myspaceMeta);
    Location namespaceLocation = namespacedLocationFactory.get(myspace.toId());
    Assert.assertTrue(namespaceLocation.exists());
    storageProviderNamespaceAdmin.delete(myspace);
    Assert.assertFalse(namespaceLocation.exists());
  }

  @Test
  public void testNSWithCustomLocation() throws Exception {
    NamespaceId customSpace = new NamespaceId("custom");
    NamespaceMeta customSpaceMeta = new NamespaceMeta.Builder().setName(customSpace.getNamespace())
      .setRootDirectory(TEMP_FOLDER.getRoot().toString() + "/" + customSpace.getNamespace()).build();
    // the create/delete will look up meta so store that too
    namespaceStore.create(customSpaceMeta);
    try {
      storageProviderNamespaceAdmin.create(customSpaceMeta);
      Assert.fail("Expected exception to be thrown while creating namespace with custom location since the custom " +
                    "location does not exist at this point.");
    } catch (IOException e) {
      // expected
    }

    // create the custom location
    File custom = TEMP_FOLDER.newFolder(customSpace.getNamespace());
    // create another directory inside this custom location and try creating the namespace with custom mapping it
    // should fail since we expect the mapped directory to be empty
    File dir1 = new File(custom, "dir1");
    Assert.assertTrue(dir1.mkdir());
    try {
      storageProviderNamespaceAdmin.create(customSpaceMeta);
      Assert.fail("Expected exception to be thrown while creating namespace with custom location since the custom " +
                    "location is not empty.");
    } catch (IOException e) {
      // expected
    }
    // delete the content of the custom location
    Assert.assertTrue(dir1.delete());

    // test failure if custom location is a file
    File randomFile = new File(custom, "file1");
    Assert.assertTrue(randomFile.createNewFile());
    try {
      storageProviderNamespaceAdmin.create(new NamespaceMeta.Builder(customSpaceMeta)
                                             .setRootDirectory(randomFile.toString()).build());
      Assert.fail("Expected exception to be thrown while creating namespace with custom location since the custom " +
                    "location is not a directory");
    } catch (IOException e) {
      // expected
    }
    // delete the file and retry creating the namespace
    Assert.assertTrue(randomFile.delete());

    storageProviderNamespaceAdmin.create(customSpaceMeta);
    // create some directories and files inside the custom mapped location
    dir1 = new File(custom, "dir1");
    Assert.assertTrue(dir1.mkdir());
    File dir2 = new File(custom, "dir2");
    Assert.assertTrue(dir2.mkdir());
    File file1 = new File(dir1, "file1");
    Assert.assertTrue(file1.createNewFile());

    // delete the namespace
    storageProviderNamespaceAdmin.delete(customSpace);
    namespaceStore.delete(customSpace.toId());
    // the data inside the custom location should have been deleted
    Assert.assertFalse("Data inside the custom location still exists.", (dir1.exists() || dir2.exists() ||
      file1.exists()));
    // but custom namespace location should still exists
    Assert.assertTrue(custom.exists());
    Assert.assertTrue(custom.delete());
  }

  @AfterClass
  public static void cleanup() throws Exception {
    transactionManager.stopAndWait();
    datasetService.stopAndWait();
  }
}
