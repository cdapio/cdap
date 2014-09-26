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

package co.cask.cdap.common.lang;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.ApplicationBundler;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Creates application jars from an application class.
 */
public final class ApplicationPackager {

  private ApplicationPackager() { }

  public static File createApplicationJar(Class<?> clz,
                                          Map<String, File> extraFiles,
                                          LocationFactory locationFactory) throws IOException {

    ApplicationBundler bundler = new ApplicationBundler(ImmutableList.of(
      "co.cask.cdap.api", "org.apache.hadoop", "org.apache.hbase", "org.apache.hive"));
    Location jarLocation = locationFactory.create(clz.getName()).getTempFile(".jar");
    bundler.createBundle(jarLocation, clz);

    Location deployJar = locationFactory.create(clz.getName()).getTempFile(".jar");

    // Creates Manifest
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(ManifestFields.MAIN_CLASS, clz.getName());

    // Create the program jar for deployment. It removes the "classes/" prefix as that's the convention taken
    // by the ApplicationBundler inside Twill.
    JarOutputStream jarOutput = new JarOutputStream(deployJar.getOutputStream(), manifest);
    try {
      JarInputStream jarInput = new JarInputStream(jarLocation.getInputStream());
      try {
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

            jarOutput.putNextEntry(jarEntry);
            if (!isDir) {
              ByteStreams.copy(jarInput, jarOutput);
            }
          }

          jarEntry = jarInput.getNextJarEntry();
        }
      } finally {
        jarInput.close();
      }

      for (Map.Entry<String, File> entry : extraFiles.entrySet()) {
        String filename = entry.getKey();
        File file = entry.getValue();

        JarEntry jarEntry = new JarEntry(filename);
        jarOutput.putNextEntry(jarEntry);
        Files.copy(file, jarOutput);
      }
    } finally {
      jarOutput.close();
    }

    return new File(deployJar.toURI());
  }

  public static File createApplicationJar(Class<?> clz,
                                          Map<String, File> extraFiles,
                                          File parentDir) throws IOException {
    LocationFactory locationFactory = new LocalLocationFactory(parentDir);
    return createApplicationJar(clz, extraFiles, locationFactory);
  }

}
