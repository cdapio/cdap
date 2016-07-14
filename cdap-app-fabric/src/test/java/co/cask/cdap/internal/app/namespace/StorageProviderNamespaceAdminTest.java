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
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.internal.guice.AppFabricTestModule;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Tests for {@link StorageProviderNamespaceAdmin}
 */
public class StorageProviderNamespaceAdminTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static NamespacedLocationFactory namespacedLocationFactory;
  private static StorageProviderNamespaceAdmin storageProviderNamespaceAdmin;

  @BeforeClass
  public static void setup() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setBoolean(Constants.Explore.EXPLORE_ENABLED, true);
    Injector injector = Guice.createInjector(new AppFabricTestModule(cConf));
    namespacedLocationFactory = injector.getInstance(NamespacedLocationFactory.class);
    storageProviderNamespaceAdmin = injector.getInstance(StorageProviderNamespaceAdmin.class);
  }

  @Test
  public void test() throws ExploreException, SQLException, IOException {
    NamespaceId myspace = new NamespaceId("myspace");
    storageProviderNamespaceAdmin.create(new NamespaceMeta.Builder().setName(myspace.getNamespace()).build());
    Location namespaceLocation = namespacedLocationFactory.get(myspace.toId());
    Assert.assertTrue(namespaceLocation.exists());
    storageProviderNamespaceAdmin.delete(myspace);
    Assert.assertFalse(namespaceLocation.exists());
  }
}
