/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib;

import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.AbstractDatasetTest;
import co.cask.cdap.data2.dataset2.DatasetNamespace;
import co.cask.cdap.data2.dataset2.lib.file.FileSetModule;
import co.cask.cdap.proto.Id;
import com.google.common.collect.Maps;
import org.apache.twill.filesystem.Location;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public class FileSetTest extends AbstractDatasetTest {

  static FileSet fileSet1;
  static FileSet fileSet2;
  private static final Id.Namespace OTHER_NAMESPACE = Id.Namespace.from("yourspace");
  private static final DatasetNamespace dsNamespace = new DefaultDatasetNamespace(CConfiguration.create());
  private static final Id.DatasetInstance testFileSetInstance1 =
    dsNamespace.namespace(Id.DatasetInstance.from(NAMESPACE_ID, "testFileSet"));
  private static final Id.DatasetInstance testFileSetInstance2 =
    dsNamespace.namespace(Id.DatasetInstance.from(OTHER_NAMESPACE, "testFileSet"));

  @BeforeClass
  public static void beforeClass() throws Exception {
    createInstance("fileSet", testFileSetInstance1, FileSetProperties.builder()
      .setBasePath("testDir").build());
    Map<String, String> fileArgs = Maps.newHashMap();
    FileSetArguments.setInputPath(fileArgs, "some?File1");
    FileSetArguments.setOutputPath(fileArgs, "some?File1");
    fileSet1 = getInstance(testFileSetInstance1, fileArgs);

    framework.addModule(Id.DatasetModule.from(OTHER_NAMESPACE, "fileSet"), new FileSetModule());
    createInstance("fileSet", testFileSetInstance2, FileSetProperties.builder()
      .setBasePath("testDir").build());
    fileArgs = Maps.newHashMap();
    FileSetArguments.setInputPath(fileArgs, "some?File2");
    FileSetArguments.setOutputPath(fileArgs, "some?File2");
    fileSet2 = getInstance(testFileSetInstance2, fileArgs);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    deleteInstance(testFileSetInstance1);
    deleteInstance(testFileSetInstance2);
  }

  @Test
  public void testWriteRead() throws IOException {
    Location fileSet1Output = fileSet1.getOutputLocation();
    Location fileSet2Output = fileSet2.getOutputLocation();
    Location fileSet1NsDir = Locations.getParent(Locations.getParent(Locations.getParent(fileSet1Output)));
    Location fileSet2NsDir = Locations.getParent(Locations.getParent(Locations.getParent(fileSet2Output)));
    Assert.assertNotNull(fileSet1NsDir);
    Assert.assertNotNull(fileSet2NsDir);
    Assert.assertEquals(fileSet1NsDir.getName(), NAMESPACE_ID.getId());
    Assert.assertEquals(fileSet2NsDir.getName(), OTHER_NAMESPACE.getId());

    Assert.assertNotEquals(fileSet1.getInputLocations().get(0).toURI().getPath(),
                           fileSet2.getInputLocations().get(0).toURI().getPath());
    Assert.assertNotEquals(fileSet1Output.toURI().getPath(), fileSet2Output.toURI().getPath());

    OutputStream out = fileSet1.getOutputLocation().getOutputStream();
    out.write(42);
    out.close();
    out = fileSet2.getOutputLocation().getOutputStream();
    out.write(54);
    out.close();

    InputStream in = fileSet1.getInputLocations().get(0).getInputStream();
    Assert.assertEquals(42, in.read());
    in.close();
    in = fileSet2.getInputLocations().get(0).getInputStream();
    Assert.assertEquals(54, in.read());
    in.close();
  }
}
