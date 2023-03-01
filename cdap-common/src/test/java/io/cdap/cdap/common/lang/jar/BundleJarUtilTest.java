/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package io.cdap.cdap.common.lang.jar;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import io.cdap.cdap.common.io.Locations;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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
    final File target = new File(TEMP_FOLDER.newFolder(), "target.jar");
    BundleJarUtil.createJar(dir, target);
    JarFile jarFile = new JarFile(target);
    Assert.assertTrue(jarFile.getJarEntry("subdir/").isDirectory());

    JarEntry jarEntry = jarFile.getJarEntry("subdir/file1");
    Assert.assertNotNull(jarEntry);
    try (Reader reader = new InputStreamReader(jarFile.getInputStream(jarEntry), Charsets.UTF_8)) {
      Assert.assertEquals(message, CharStreams.toString(reader));
    }
  }

  @Test
  public void testPrepareClassLoader() throws IOException {
    // Create a file inside a sub-dir.
    File dir = TEMP_FOLDER.newFolder();
    File subDir = new File(dir, "subdir");
    subDir.mkdirs();

    String message = Strings.repeat("0123456789", 40);
    File file1 = new File(subDir, "file1.jar");
    Files.write(message, file1, Charsets.UTF_8);

    // Create a jar of the top level directory
    final File destArchive = new File(TEMP_FOLDER.newFolder(), "target.jar");
    BundleJarUtil.createJar(dir, destArchive);

    // Unpack the jar into a folder
    File unpackedDir = BundleJarUtil.prepareClassLoaderFolder(Locations.toLocation(destArchive),
                                                              TEMP_FOLDER::newFolder).getDir();

    // Unpack from the unpacked folder again. It should return the same folder
    try (ClassLoaderFolder classLoaderFolder = BundleJarUtil.prepareClassLoaderFolder(unpackedDir,
                                                                                      TEMP_FOLDER::newFolder)) {
      Assert.assertEquals(unpackedDir, classLoaderFolder.getDir());
    }

    // Closing the ClassLoaderFolder should have the original unpackedDir retained
    Assert.assertTrue(unpackedDir.exists());
  }

  @Test
  public void testPackFolderWithFileNameFilter() throws IOException {
    Predicate<String> fileNameFilter = new Predicate<String>() {
      @Override
      public boolean test(String name) {
        return name.startsWith("abcd");
      }
    };

    File inputFolder = TEMP_FOLDER.newFolder();
    File inputExcluded = File.createTempFile("abcd", "txt", inputFolder);
    File inputIncluded = File.createTempFile("efgh", "txt", inputFolder);
    Set<String> expectedFiles = new HashSet<>();
    expectedFiles.add(inputIncluded.getName());

    File destArchive = new File(TEMP_FOLDER.newFolder(), "myBundle.jar");
    try (JarOutputStream jarOut = new JarOutputStream(new FileOutputStream(destArchive))) {
      BundleJarUtil.addToArchive(inputFolder, false, jarOut, fileNameFilter);
    }

    try (JarFile jarFile = new JarFile(destArchive)) {
      Assert.assertTrue(jarFile.stream().map(JarEntry::getName).allMatch(expectedFiles::contains));
    }
  }

  @Test
  public void testPackFileWithFileNameFilter() throws IOException {
    Predicate<String> fileNameFilter = new Predicate<String>() {
      @Override
      public boolean test(String name) {
        return name.startsWith("abcd");
      }
    };

    File inputExcluded = File.createTempFile("abcd", "txt", TEMP_FOLDER.newFolder());

    File destArchive = new File(TEMP_FOLDER.newFolder(), "myBundle.jar");
    try (JarOutputStream jarOut = new JarOutputStream(new FileOutputStream(destArchive))) {
      BundleJarUtil.addToArchive(inputExcluded, false, jarOut, fileNameFilter);
    }

    try (JarFile jarFile = new JarFile(destArchive)) {
      Assert.assertEquals(0, jarFile.stream().count());
    }
  }

  /**
   * Helper method to recursively check if two directories are equal
   */
  private void areDirectoriesEqual(File dir1, File dir2) {
    File[] dir1Files = dir1.listFiles();
    File[] dir2Files = dir2.listFiles();
    Assert.assertNotNull(dir1Files);
    Assert.assertNotNull(dir2Files);
    Assert.assertEquals(dir1Files.length, dir2Files.length);
    for (int i = 0; i < dir1Files.length; i++) {
      if (dir1Files[i].isDirectory()) {
        areDirectoriesEqual(dir1Files[i], dir2Files[i]);
        continue;
      }
      Assert.assertEquals(dir1Files[i].getName(), dir2Files[i].getName());
    }
  }

  private void testNumFiles(int numFiles, boolean isFile) throws IOException {
    File input = isFile ? File.createTempFile("abcd", "txt", TEMP_FOLDER.newFolder()) : TEMP_FOLDER.newFolder();
    Set<String> files = new HashSet<>();
    if (!isFile) {
      for (int i = 0; i < numFiles; i++) {
        files.add(File.createTempFile("abcd", ".txt", input).getName());
      }
    } else {
      files.add(input.getName());
    }

    File destArchive = new File(TEMP_FOLDER.newFolder(), "myBundle.jar");
    BundleJarUtil.createJar(input, destArchive);

    try (JarFile jarFile = new JarFile(destArchive)) {
      Assert.assertTrue(jarFile.stream().map(JarEntry::getName).allMatch(files::contains));
    }
  }
}
