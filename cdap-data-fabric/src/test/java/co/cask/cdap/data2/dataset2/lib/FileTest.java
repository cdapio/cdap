/*
 * Copyright 2014 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.File;
import co.cask.cdap.data2.dataset2.AbstractDatasetTest;
import com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class FileTest extends AbstractDatasetTest {

  static File file;

  @BeforeClass
  public static void beforeClass() throws Exception {
    createInstance("file", "testFileSet", DatasetProperties.builder()
      .add(File.PROPERTY_BASE_PATH, "testDir").build());
    file = getInstance("testFileSet", ImmutableMap.of(File.ARGUMENT_INPUT_PATHS, "some?File",
                                                      File.ARGUMENT_OUTPUT_PATH, "some?File"));
  }

  @AfterClass
  public static void afterClass() throws Exception {
    deleteInstance("testFileSet");
  }

  @Test
  public void testWriteRead() throws IOException {
    OutputStream out = file.getOutputLocation().getOutputStream();
    out.write(42);
    out.close();

    InputStream in = file.getInputLocations().get(0).getInputStream();
    Assert.assertEquals(42, in.read());
    in.close();
  }

}
