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

package co.cask.cdap.common.lang.jar;

import co.cask.cdap.common.io.Locations;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit Tests for {@link BundleJarUtil}.
 */
public class BundleJarUtilTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testPackingDir() throws IOException {
    testNumFiles(10, false);
  }

  @Test
  public void testPackingEmptyDir() throws IOException {
    testNumFiles(0, false);
  }

  @Test
  public void testPackingFile() throws IOException {
    testNumFiles(1, true);
  }

  private void testNumFiles(int numFiles, boolean isFile) throws IOException {
    File input = isFile ? File.createTempFile("abcd", "txt", tmpFolder.newFolder()) : tmpFolder.newFolder();
    List<File> files = new ArrayList<>();
    if (!isFile) {
      for (int i = 0; i < numFiles; i++) {
        files.add(File.createTempFile("abcd", "txt", input));
      }
    } else {
      files.add(input);
    }

    File destArchive = new File(tmpFolder.newFolder(), "myBundle.jar");
    BundleJarUtil.packDirFiles(input, Locations.toLocation(destArchive), tmpFolder.newFolder());
    for (File file : files) {
      BundleJarUtil.getEntry(Locations.toLocation(destArchive), file.getName()).getInput().close();
    }
  }
}
