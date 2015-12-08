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
import co.cask.cdap.proto.Id;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Tests for {@link DefaultNamespacedLocationFactory}.
 */
public class DefaultNamespacedLocationFactoryTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testGet() throws IOException {
    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    NamespacedLocationFactory namespacedLocationFactory =
      new DefaultNamespacedLocationFactory(CConfiguration.create(), locationFactory);

    Location defaultLoc = namespacedLocationFactory.get(Id.Namespace.DEFAULT);
    Id.Namespace ns1 = Id.Namespace.from("ns1");
    Location ns1Loc = namespacedLocationFactory.get(ns1);

    // test these are not the same
    Assert.assertNotEquals(defaultLoc, ns1Loc);

    // test subdirectories in a namespace
    Location sub1 = namespacedLocationFactory.get(ns1, "sub1");
    Location sub2 = namespacedLocationFactory.get(ns1, "sub2");
    Assert.assertNotEquals(sub1, sub2);
  }
}
