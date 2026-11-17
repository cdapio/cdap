/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.common.lang;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link DirectoryClassLoader}.
 */
public class DirectoryClassLoaderTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testManifestLibDirsAndRewriteUnsupported() throws Exception {
    File baseDir = TEMP_FOLDER.newFolder();
    File libDir = new File(baseDir, "lib");
    Assert.assertTrue(libDir.mkdirs());
    File sampleJar = new File(libDir, "sample.jar");
    Assert.assertTrue(sampleJar.createNewFile());

    File manifestFile = new File(baseDir, JarFile.MANIFEST_NAME.replace('/', File.separatorChar));
    Assert.assertTrue(manifestFile.getParentFile().mkdirs());
    Manifest writtenManifest = new Manifest();
    writtenManifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    writtenManifest.getMainAttributes().putValue("Custom-Header", "cdap-test");
    try (FileOutputStream fos = new FileOutputStream(manifestFile)) {
      writtenManifest.write(fos);
    }

    try (DirectoryClassLoader loader = new DirectoryClassLoader(baseDir, null, "lib")) {
      Assert.assertNotNull(loader.getManifest());
      Assert.assertEquals("cdap-test",
          loader.getManifest().getMainAttributes().getValue("Custom-Header"));
      Assert.assertFalse(loader.needIntercept("io.cdap.Sample"));

      List<String> urlStrings = Arrays.stream(loader.getURLs())
          .map(Object::toString)
          .collect(Collectors.toList());
      Assert.assertTrue(urlStrings.stream().anyMatch(u -> u.endsWith("sample.jar")));

      try {
        loader.rewriteClass("io.cdap.Sample", new ByteArrayInputStream(new byte[0]));
        Assert.fail("Expected UnsupportedOperationException");
      } catch (UnsupportedOperationException expected) {
        // expected
      }
    }

    // Directory without MANIFEST.MF yields null getManifest().
    try (DirectoryClassLoader emptyLoader = new DirectoryClassLoader(
        TEMP_FOLDER.newFolder(), null)) {
      Assert.assertNull(emptyLoader.getManifest());
    }
  }

  @Test
  public void testMalformedUrlPropagatesAsRuntimeException() throws Exception {
    File badSchemeDir = new File(TEMP_FOLDER.newFolder(), "bad") {
      @Override
      public URI toURI() {
        return URI.create("unknownproto://bad/dir");
      }
    };

    try {
      new DirectoryClassLoader(badSchemeDir, null);
      Assert.fail("Expected RuntimeException wrapping MalformedURLException");
    } catch (RuntimeException caught) {
      Assert.assertTrue(caught.getCause() instanceof MalformedURLException);
    }
  }
}
