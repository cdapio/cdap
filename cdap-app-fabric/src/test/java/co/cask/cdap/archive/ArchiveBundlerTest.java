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

package co.cask.cdap.archive;

import co.cask.cdap.WebCrawlApp;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.lang.jar.JarFinder;
import co.cask.cdap.common.lang.jar.JarResources;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * This is test for archive bundler.
 * Ignored for now till we figure out how to create a archive.
 */
public class ArchiveBundlerTest {

  @Test
  public void testBundler() throws Exception {
    LocationFactory lf = new LocalLocationFactory();
    Location out = lf.create(File.createTempFile("testBundler", ".jar").toURI());

    try {
      Manifest manifest = new Manifest();
      manifest.getMainAttributes().put(ManifestFields.MANIFEST_VERSION, "1.0");
      manifest.getMainAttributes().put(ManifestFields.MAIN_CLASS, "co.cask.cdap.WebCrawlApp");
      manifest.getMainAttributes().put(ManifestFields.PROGRAM_TYPE, "FLOW");
      manifest.getMainAttributes().put(ManifestFields.SPEC_FILE, "META-INF/specification/application.json");

      // Create a JAR file based on the class.
      Location jarfile = lf.create(JarFinder.getJar(WebCrawlApp.class));

      // Create a bundler.
      ArchiveBundler bundler = new ArchiveBundler(jarfile);

      // Create a bundle with modified manifest and added application.json.

      bundler.clone(out, manifest, ImmutableMap.of("application.json",
                                                   ByteStreams.newInputStreamSupplier("{}".getBytes(Charsets.UTF_8))));
      Assert.assertTrue(out.exists());
      JarFile file = new JarFile(new File(out.toURI()));
      Enumeration<JarEntry> entries = file.entries();

      Manifest newManifest = file.getManifest();
      Assert.assertTrue(newManifest.getMainAttributes().get(ManifestFields.MANIFEST_VERSION).equals("1.0"));
      Assert.assertTrue(newManifest.getMainAttributes().get(ManifestFields.MAIN_CLASS)
                          .equals("co.cask.cdap.WebCrawlApp"));
      Assert.assertTrue(newManifest.getMainAttributes().get(ManifestFields.PROGRAM_TYPE).equals("FLOW"));
      Assert.assertTrue(newManifest.getMainAttributes().get(ManifestFields.SPEC_FILE)
                          .equals("META-INF/specification/application.json"));

      JarResources oldJar = new JarResources(jarfile);

      boolean foundAppJson = false;
      while (entries.hasMoreElements()) {
        JarEntry entry = entries.nextElement();

        if (entry.getName().contains("application.json")) {
          foundAppJson = true;
        } else if (!entry.isDirectory() && !entry.getName().equals(JarFile.MANIFEST_NAME)) {
          Assert.assertNotNull(oldJar.getResource(entry.getName()));
        }
      }
      Assert.assertTrue(foundAppJson);
    } finally {
      out.delete();
    }
  }
}
