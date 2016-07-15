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

package co.cask.cdap.common.namespace;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.RootLocationFactory;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link DefaultNamespacedLocationFactory}.
 */
public class DefaultNamespacedLocationFactoryTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testGet() throws Exception {
    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    NamespaceAdmin nsAdmin = new InMemoryNamespaceClient();

    Id.Namespace ns1 = Id.Namespace.from("ns1");
    NamespaceMeta defaultNSMeta = new NamespaceMeta.Builder().setName(Id.Namespace.DEFAULT).build();
    NamespaceMeta ns1NSMeta = new NamespaceMeta.Builder().setName(ns1).setRootDirectory("/ns1").build();

    nsAdmin.create(defaultNSMeta);
    nsAdmin.create(ns1NSMeta);

    CConfiguration cConf = CConfiguration.create();
    NamespacedLocationFactory namespacedLocationFactory =
      new DefaultNamespacedLocationFactory(cConf, new RootLocationFactory(locationFactory), nsAdmin);

    Location defaultLoc = namespacedLocationFactory.get(Id.Namespace.DEFAULT);
    Location ns1Loc = namespacedLocationFactory.get(ns1);

    // check if custom mapping location was as expected
    Location expectedLocation = namespacedLocationFactory.getBaseLocation().append("/ns1");
    Assert.assertEquals(expectedLocation, ns1Loc);

    String tempLocationRoot = namespacedLocationFactory.getBaseLocation().toString();

    // check that the namespace which does not have custom mapping is created under namespace and namespace dir
    String defaultNsPath = defaultLoc.toString().split(tempLocationRoot)[1];
    String ns1Path = ns1Loc.toString().split(tempLocationRoot)[1];

    Assert.assertTrue(defaultNsPath.contains(cConf.get(Constants.CFG_HDFS_NAMESPACE)) &&
                        defaultNsPath.contains(cConf.get(Constants.Namespace.NAMESPACES_DIR)));

    // check that the namespace which has custom mapping is not created under namespace and namespace dir
    Assert.assertFalse(ns1Path.contains(cConf.get(Constants.CFG_HDFS_NAMESPACE)) &&
                         ns1Path.contains(cConf.get(Constants.Namespace.NAMESPACES_DIR)));

    // test these are not the same
    Assert.assertNotEquals(defaultLoc, ns1Loc);

    // test subdirectories in a namespace
    Location sub1 = namespacedLocationFactory.get(ns1, "sub1");
    Location sub2 = namespacedLocationFactory.get(ns1, "sub2");
    Assert.assertNotEquals(sub1, sub2);
  }
}
