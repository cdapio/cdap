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
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Unit Tests for {@link BundleJarUtil}.
 */
public class BundleJarUtilTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

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

  @Test
  public void testRecursive() throws IOException {
    // Create a file inside a sub-dir.
    File dir = TEMP_FOLDER.newFolder();
    File subDir = new File(dir, "subdir");
    subDir.mkdirs();

    String message = Strings.repeat("0123456789", 40);
    File file1 = new File(subDir, "file1");
    Files.write(message, file1, Charsets.UTF_8);

    // Create a jar of the top level directory
    File target = new File(TEMP_FOLDER.newFolder(), "target.jar");
    BundleJarUtil.createJar(dir, Files.newOutputStreamSupplier(target));

    JarFile jarFile = new JarFile(target);
    Assert.assertTrue(jarFile.getJarEntry("subdir/").isDirectory());

    JarEntry jarEntry = jarFile.getJarEntry("subdir/file1");
    Assert.assertNotNull(jarEntry);
    try (Reader reader = new InputStreamReader(jarFile.getInputStream(jarEntry), Charsets.UTF_8)) {
      Assert.assertEquals(message, CharStreams.toString(reader));
    }
  }

  private void testNumFiles(int numFiles, boolean isFile) throws IOException {
    File input = isFile ? File.createTempFile("abcd", "txt", TEMP_FOLDER.newFolder()) : TEMP_FOLDER.newFolder();
    List<File> files = new ArrayList<>();
    if (!isFile) {
      for (int i = 0; i < numFiles; i++) {
        files.add(File.createTempFile("abcd", "txt", input));
      }
    } else {
      files.add(input);
    }

    File destArchive = new File(TEMP_FOLDER.newFolder(), "myBundle.jar");
    BundleJarUtil.createJar(input, destArchive);
    for (File file : files) {
      BundleJarUtil.getEntry(Locations.toLocation(destArchive), file.getName()).getInput().close();
    }
  }
}
