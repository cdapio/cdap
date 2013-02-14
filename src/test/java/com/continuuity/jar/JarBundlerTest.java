package com.continuuity.jar;

import java.io.File;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * This is test for jar bundler.
 * Ignored for now till we figure out how to create a jar.
 */
public class JarBundlerTest {

  public void testBundler() throws Exception {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, "foo.testBundler");
    manifest.getMainAttributes().put(new Attributes.Name("Processor-Type"), "FLOW");
    manifest.getMainAttributes().put(new Attributes.Name("Spec-File"), "META-INF/specification/application.json");
    JarBundler bundler = new JarBundler(new File("/tmp/TwitterFlow-1.0-jar-with-dependencies.jar"));
    bundler.clone(new File("/tmp/1.jar"), manifest, new File[]{ new File("/tmp/application.json") });
  }
}
