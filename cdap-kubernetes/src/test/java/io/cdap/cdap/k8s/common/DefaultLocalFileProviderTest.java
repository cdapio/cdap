/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.k8s.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for DefaultLocalFileProvider.
 */
public class DefaultLocalFileProviderTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  /**
   * Appends random byte data to an empty file, reads it back, and asserts it is the same as previously written.
   */
  private void assertWrittenToFile(File file) throws IOException {
    Random random = new Random();
    byte[] expectedRandBytes = new byte[256];
    random.nextBytes(expectedRandBytes);
    try (FileOutputStream outputStream = new FileOutputStream(file)) {
      outputStream.write(expectedRandBytes);
    }
    byte[] receivedBytes = new byte[256];
    try (FileInputStream inputStream = new FileInputStream(file)) {
      inputStream.read(receivedBytes);
    }
    Assert.assertArrayEquals(expectedRandBytes, receivedBytes);
  }

  @Test
  public void testAbsolutePathFileIsWritable() throws Exception {
    Path filePath = temporaryFolder.getRoot().toPath().resolve("file.txt");
    DefaultLocalFileProvider defaultLocalFileProvider = new DefaultLocalFileProvider();
    File file = defaultLocalFileProvider.getWritableFileRef(filePath.toString());
    assertWrittenToFile(file);
  }

  @Test
  public void testAbsolutePathFileWithNonexistentParentDirectoryIsIsWritable() throws Exception {
    Path filePath = temporaryFolder.getRoot().toPath().resolve("tmp/abc/file.txt");
    DefaultLocalFileProvider defaultLocalFileProvider = new DefaultLocalFileProvider();
    File file = defaultLocalFileProvider.getWritableFileRef(filePath.toString());
    assertWrittenToFile(file);
  }

  @Test
  public void testMultipleAbsolutePathFilesWithNonexistentParentDirectoryAreWritable() throws Exception {
    DefaultLocalFileProvider defaultLocalFileProvider = new DefaultLocalFileProvider();
    Path filePath1 = temporaryFolder.getRoot().toPath().resolve("tmp/abc/dec/file1.txt");
    Path filePath2 = temporaryFolder.getRoot().toPath().resolve("tmp/abc/dec/file2.txt");
    Path filePath3 = temporaryFolder.getRoot().toPath().resolve("tmp/abc/file3.txt");
    Path filePath4 = temporaryFolder.getRoot().toPath().resolve("tmp/file4.txt");
    File file1 = defaultLocalFileProvider.getWritableFileRef(filePath1.toString());
    File file2 = defaultLocalFileProvider.getWritableFileRef(filePath2.toString());
    File file3 = defaultLocalFileProvider.getWritableFileRef(filePath3.toString());
    File file4 = defaultLocalFileProvider.getWritableFileRef(filePath4.toString());
    assertWrittenToFile(file1);
    assertWrittenToFile(file2);
    assertWrittenToFile(file3);
    assertWrittenToFile(file4);
  }
}
