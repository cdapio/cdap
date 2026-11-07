/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.k8s.runtime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link FileLocalizer}.
 */
public class FileLocalizerTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private Path invokeExpand(InputStream zipInputStream, Path targetDir) throws Exception {
    FileLocalizer fileLocalizer = new FileLocalizer(null, null);
    Method expandMethod = FileLocalizer.class.getDeclaredMethod(
        "expand", java.net.URI.class, InputStream.class, Path.class);
    expandMethod.setAccessible(true);
    try {
      return (Path) expandMethod.invoke(fileLocalizer, java.net.URI.create("test:///archive.zip"),
          zipInputStream, targetDir);
    } catch (java.lang.reflect.InvocationTargetException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw e;
    }
  }

  private byte[] createZip(String entryName, String content) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ZipOutputStream zos = new ZipOutputStream(baos)) {
      zos.putNextEntry(new ZipEntry(entryName));
      zos.write(content.getBytes(StandardCharsets.UTF_8));
      zos.closeEntry();
    }
    return baos.toByteArray();
  }

  @Test
  public void testExpandRejectsPathTraversalEntry() throws Exception {
    Path targetDir = temporaryFolder.newFolder("pod-workdir", "archive-target").toPath();
    byte[] maliciousZip = createZip("../../pwned_by_zipslip.txt", "attacker controlled content");

    try {
      invokeExpand(new ByteArrayInputStream(maliciousZip), targetDir);
      Assert.fail("Expected IOException for a zip entry escaping the target directory");
    } catch (IOException expected) {
      Assert.assertTrue(expected.getMessage().contains("Illegal path detected"));
    }

    File escapedFile = temporaryFolder.getRoot().toPath().resolve("pwned_by_zipslip.txt").toFile();
    Assert.assertFalse("File must not be written outside the target directory",
        escapedFile.exists());
  }

  @Test
  public void testExpandAllowsNormalEntry() throws Exception {
    Path targetDir = temporaryFolder.newFolder("pod-workdir", "archive-target").toPath();
    byte[] normalZip = createZip("config/settings.txt", "normal content");

    invokeExpand(new ByteArrayInputStream(normalZip), targetDir);

    Path expandedFile = targetDir.resolve("config/settings.txt");
    Assert.assertTrue("Legitimate entry should still be extracted", Files.exists(expandedFile));
    Assert.assertEquals("normal content",
        new String(Files.readAllBytes(expandedFile), StandardCharsets.UTF_8));
  }
}
