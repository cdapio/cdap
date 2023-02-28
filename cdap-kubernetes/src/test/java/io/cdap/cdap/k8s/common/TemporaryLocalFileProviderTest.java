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
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for TemporaryLocalFileProvider.
 */
public class TemporaryLocalFileProviderTest {

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
  public void testRelativePathFileIsWritableAndCleanedUp() throws Exception {
    TemporaryFolder temporaryFolder = new TemporaryFolder();
    temporaryFolder.create();
    TemporaryLocalFileProvider temporaryLocalFileProvider = new TemporaryLocalFileProvider(temporaryFolder);
    File temporaryFile = temporaryLocalFileProvider.getWritableFileRef("file.txt");
    assertWrittenToFile(temporaryFile);
    temporaryFolder.delete();
    Assert.assertFalse(temporaryFile.exists());
  }

  @Test
  public void testAbsolutePathFileIsIsWritableAndCleanedUp() throws Exception {
    TemporaryFolder temporaryFolder = new TemporaryFolder();
    temporaryFolder.create();
    TemporaryLocalFileProvider temporaryLocalFileProvider = new TemporaryLocalFileProvider(temporaryFolder);
    File temporaryFile = temporaryLocalFileProvider.getWritableFileRef("/tmp/abc/dec/file.txt");
    assertWrittenToFile(temporaryFile);
    temporaryFolder.delete();
    Assert.assertFalse(temporaryFile.exists());
  }

  @Test
  public void testMultipleAbsolutePathFilesAreWritableAndCleanedUp() throws Exception {
    TemporaryFolder temporaryFolder = new TemporaryFolder();
    temporaryFolder.create();
    TemporaryLocalFileProvider temporaryLocalFileProvider = new TemporaryLocalFileProvider(temporaryFolder);
    File temporaryFile1 = temporaryLocalFileProvider.getWritableFileRef("/tmp/abc/dec/file1.txt");
    File temporaryFile2 = temporaryLocalFileProvider.getWritableFileRef("/tmp/abc/dec/file2.txt");
    File temporaryFile3 = temporaryLocalFileProvider.getWritableFileRef("/tmp/abc/file3.txt");
    File temporaryFile4 = temporaryLocalFileProvider.getWritableFileRef("/tmp/file4.txt");
    assertWrittenToFile(temporaryFile1);
    assertWrittenToFile(temporaryFile2);
    assertWrittenToFile(temporaryFile3);
    assertWrittenToFile(temporaryFile4);
    temporaryFolder.delete();
    Assert.assertFalse(temporaryFile1.exists());
    Assert.assertFalse(temporaryFile2.exists());
    Assert.assertFalse(temporaryFile3.exists());
    Assert.assertFalse(temporaryFile4.exists());
  }
}
