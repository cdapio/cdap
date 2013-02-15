package com.continuuity.archive;

import com.continuuity.filesystem.Location;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.internal.filesystem.LocalLocationFactory;

import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * This is test for archive bundler.
 * Ignored for now till we figure out how to create a archive.
 */
public class ArchiveBundlerTest {

  public void testBundler() throws Exception {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, "foo.testBundler");
    manifest.getMainAttributes().put(new Attributes.Name("Processor-Type"), "FLOW");
    manifest.getMainAttributes().put(new Attributes.Name("Spec-File"), "META-INF/specification/application.json");
    LocationFactory lf = new LocalLocationFactory();
    ArchiveBundler bundler = new ArchiveBundler(lf.create("/tmp/TwitterFlow-1.0-archive-with-dependencies.archive"));
    bundler.clone(lf.create("/tmp/1.archive"), manifest, new Location[]{ lf.create("/tmp/application.json") });
  }
}
