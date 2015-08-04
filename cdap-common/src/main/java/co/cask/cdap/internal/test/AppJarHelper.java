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

package co.cask.cdap.internal.test;

import co.cask.cdap.common.lang.ClassLoaders;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.ApplicationBundler;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Helper class for building application jar.
 */
public final class AppJarHelper {

  private AppJarHelper() {
    // No-op
  }

  public static Location createDeploymentJar(LocationFactory locationFactory, Class<?> clz, Manifest manifest,
                                             File... bundleEmbeddedJars) throws IOException {

    ApplicationBundler bundler = new ApplicationBundler(ImmutableList.of("co.cask.cdap.api",
                                                                         "org.apache.hadoop",
                                                                         "org.apache.hive",
                                                                         "org.apache.spark"),
                                                        ImmutableList.of("org.apache.hadoop.hbase"));
    Location jarLocation = locationFactory.create(clz.getName()).getTempFile(".jar");
    ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(clz.getClassLoader());
    try {
      bundler.createBundle(jarLocation, clz);
    } finally {
      ClassLoaders.setContextClassLoader(oldClassLoader);
    }

    Location deployJar = locationFactory.create(clz.getName()).getTempFile(".jar");
    Manifest jarManifest = new Manifest(manifest);
    jarManifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    jarManifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, clz.getName());


    // Create the program jar for deployment. It removes the "classes/" prefix as that's the convention taken
    // by the ApplicationBundler inside Twill.
    Set<String> seenEntries = new HashSet<>();
    try (
      JarOutputStream jarOutput = new JarOutputStream(deployJar.getOutputStream(), jarManifest);
      JarInputStream jarInput = new JarInputStream(jarLocation.getInputStream())
    ) {
      JarEntry jarEntry = jarInput.getNextJarEntry();
      while (jarEntry != null) {
        boolean isDir = jarEntry.isDirectory();
        String entryName = jarEntry.getName();
        if (!entryName.equals("classes/")) {
          if (entryName.startsWith("classes/")) {
            jarEntry = new JarEntry(entryName.substring("classes/".length()));
          } else {
            jarEntry = new JarEntry(entryName);
          }

          // TODO: this is due to manifest possibly already existing in the jar, but we also
          // create a manifest programatically so it's possible to have a duplicate entry here
          if ("META-INF/MANIFEST.MF".equalsIgnoreCase(jarEntry.getName())) {
            jarEntry = jarInput.getNextJarEntry();
            continue;
          }

          if (seenEntries.add(jarEntry.getName())) {
            jarOutput.putNextEntry(jarEntry);
            if (!isDir) {
              ByteStreams.copy(jarInput, jarOutput);
            }
          }
        }

        jarEntry = jarInput.getNextJarEntry();
      }

      for (File embeddedJar : bundleEmbeddedJars) {
        jarEntry = new JarEntry("lib/" + embeddedJar.getName());
        if (seenEntries.add(jarEntry.getName())) {
          jarOutput.putNextEntry(jarEntry);
          Files.copy(embeddedJar, jarOutput);
        }
      }
    }

    return deployJar;
  }

  public static Location createDeploymentJar(LocationFactory locationFactory,
                                             Class<?> clz, File... bundleEmbeddedJars) throws IOException {
    // Creates Manifest
    Manifest manifest = new Manifest();
    return createDeploymentJar(locationFactory, clz, manifest, bundleEmbeddedJars);
  }
}
