package com.continuuity.archive;

import com.continuuity.WebCrawlApp;
import com.continuuity.app.program.ManifestFields;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.filesystem.Location;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.internal.filesystem.LocalLocationFactory;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.OutputStream;
import java.nio.charset.Charset;
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
    OutputStream of = null;
    LocationFactory lf = new LocalLocationFactory();
    Location f = lf.create("/tmp/application.json");
    Location out = lf.create("/tmp/testBundler." + System.currentTimeMillis() + ".jar");

    try {
      Manifest manifest = new Manifest();
      manifest.getMainAttributes().put(ManifestFields.MANIFEST_VERSION, "1.0");
      manifest.getMainAttributes().put(ManifestFields.MAIN_CLASS, "com.continuuity.WebCrawlApp");
      manifest.getMainAttributes().put(ManifestFields.PROCESSOR_TYPE, "FLOW");
      manifest.getMainAttributes().put(ManifestFields.SPEC_FILE, "META-INF/specification/application.json");

      // Create a JAR file based on the class.
      String jarfile = JarFinder.getJar(WebCrawlApp.class);

      // Create an application json.
      of = f.getOutputStream();
      of.write("{}".getBytes(Charset.forName("UTF8")));

      // Create a bundler.
      ArchiveBundler bundler = new ArchiveBundler(lf.create(jarfile));

      // Create a bundle with modified manifest and added application.json.
      bundler.clone(out, manifest, ImmutableList.of(new ImmutablePair<String, Location>("application.json", f)));
      Assert.assertTrue(out.exists());
      JarFile file = new JarFile(new File(out.toURI()));
      Enumeration<JarEntry> entries = file.entries();

      Manifest newManifest = file.getManifest();
      Assert.assertTrue(newManifest.getMainAttributes().get(ManifestFields.MANIFEST_VERSION).equals("1.0"));
      Assert.assertTrue(newManifest.getMainAttributes().get(ManifestFields.MAIN_CLASS)
                          .equals("com.continuuity.WebCrawlApp"));
      Assert.assertTrue(newManifest.getMainAttributes().get(ManifestFields.PROCESSOR_TYPE).equals("FLOW"));
      Assert.assertTrue(newManifest.getMainAttributes().get(ManifestFields.SPEC_FILE)
                          .equals("META-INF/specification/application.json"));
      boolean found_app_json = false;
      while(entries.hasMoreElements()) {
        JarEntry entry = entries.nextElement();
        if(entry.getName().contains("application.json")){
          found_app_json = true;
          break;
        }
      }
      Assert.assertTrue(found_app_json);
    } finally {
      if(of != null) {
        of.close();
        f.delete();
      }
      out.delete();
    }
  }
}
