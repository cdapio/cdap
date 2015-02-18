/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.data2.dataset2.AbstractDatasetTest;
import co.cask.cdap.proto.Id;
import com.google.common.collect.Maps;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public class FileSetTest extends AbstractDatasetTest {

  static FileSet fileSet;
  private static final Id.DatasetInstance testFileSetInstance = Id.DatasetInstance.from(NAMESPACE_ID, "testFileSet");

  @BeforeClass
  public static void beforeClass() throws Exception {
    createInstance("fileSet", testFileSetInstance, FileSetProperties.builder()
      .setBasePath("testDir").build());
    Map<String, String> fileArgs = Maps.newHashMap();
    FileSetArguments.setInputPath(fileArgs, "some?File");
    FileSetArguments.setOutputPath(fileArgs, "some?File");
    fileSet = getInstance(testFileSetInstance, fileArgs);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    deleteInstance(testFileSetInstance);
  }

  @Test
  public void testWriteRead() throws IOException {
    OutputStream out = fileSet.getOutputLocation().getOutputStream();
    out.write(42);
    out.close();

    InputStream in = fileSet.getInputLocations().get(0).getInputStream();
    Assert.assertEquals(42, in.read());
    in.close();
  }

}
