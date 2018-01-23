/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.explore.service.datasets;

import co.cask.cdap.hive.stream.StreamStorageHandler;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.common.io.OutputSupplier;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.utils.Dependencies;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 *
 */
public class FatJarTest {

  private static final Logger LOG = LoggerFactory.getLogger(FatJarTest.class);

  @Test
  public void test() throws IOException, ClassNotFoundException {
    Location location = ensureCoprocessorExists(true);
    int i = 2;
  }

  /**
   * Get the location of the coprocessor and ensure it exists, optionally overwriting it if it exists.
   * In distributed mode, the coprocessor jar is loaded onto hdfs by the CoprocessorBuildTool,
   * but in other modes it is still useful to create the jar on demand.
   *
   * @param overwrite whether to overwrite the coprocessor if it already exists
   * @return the location of the coprocessor
   * @throws IOException if there was an issue accessing the location
   */
  public synchronized Location ensureCoprocessorExists(boolean overwrite) throws IOException, ClassNotFoundException {

    File tmpDir = Files.createTempDir();

    LocalLocationFactory localLocationFactory = new LocalLocationFactory(tmpDir);

    final Location jarDependencyFile = localLocationFactory.create("dependency.jar");
    if (!overwrite && jarDependencyFile.exists()) {
      return jarDependencyFile;
    }

    // ensure the jar directory exists
//    Locations.mkdirsIfNotExists(jarDir); // not needed because of Files.createTempDir() above


    LOG.debug("Creating jar file.");

//    HBaseTableUtilFactory.getHBaseTableUtilClass()

    final Map<String, URL> dependentClasses = new HashMap<>();
    for (Class<?> clz : ImmutableSet.of(StreamStorageHandler.class)) {
      Dependencies.findClassDependencies(clz.getClassLoader(), new ClassAcceptor() {
        @Override
        public boolean accept(String className, final URL classUrl, URL classPathUrl) {
          // Assuming the endpoint and protocol class doesn't have dependencies
          // other than those comes with HBase, Java, fastutil, and gson
//          if (className.startsWith("co.cask") || className.startsWith("it.unimi.dsi.fastutil")
//            || className.startsWith("org.apache.tephra") || className.startsWith("com.google.gson")) {
            if (!dependentClasses.containsKey(className)) {
              dependentClasses.put(className, classUrl);
            }
            return true;
//          }
//          return false;
        }
      }, clz.getName());
    }

    if (dependentClasses.isEmpty()) {
      return null;
    }

    // create the coprocessor jar on local filesystem
    LOG.debug("Adding " + dependentClasses.size() + " classes to jar");
    File jarFile = File.createTempFile("coprocessor", ".jar");
    byte[] buffer = new byte[4 * 1024];
    try (JarOutputStream jarOutput = new JarOutputStream(new FileOutputStream(jarFile))) {
      for (Map.Entry<String, URL> entry : dependentClasses.entrySet()) {
        jarOutput.putNextEntry(new JarEntry(entry.getKey().replace('.', File.separatorChar) + ".class"));

        try (InputStream inputStream = entry.getValue().openStream()) {
          int len = inputStream.read(buffer);
          while (len >= 0) {
            jarOutput.write(buffer, 0, len);
            len = inputStream.read(buffer);
          }
        }
      }
    } catch (IOException e) {
      LOG.error("Unable to create temporary local coprocessor jar {}.", jarFile.getAbsolutePath(), e);
      if (!jarFile.delete()) {
        LOG.warn("Unable to clean up temporary local coprocessor jar {}.", jarFile.getAbsolutePath());
      }
      throw e;
    }

    // copy the local jar file to the filesystem (HDFS)
    // copies to a tmp location then renames the tmp location to the target location in case
    // multiple CoprocessorManagers we called at the same time. This should never be the case in distributed
    // mode, as coprocessors should all be loaded beforehand using the CoprocessorBuildTool.
    final Location tmpLocation = jarDependencyFile.getTempFile(".jar");
    try {
      // Copy jar file into filesystem (HDFS)
      Files.copy(jarFile, new OutputSupplier<OutputStream>() {
        @Override
        public OutputStream getOutput() throws IOException {
          return tmpLocation.getOutputStream();
        }
      });
    } catch (IOException e) {
      LOG.error("Unable to copy local coprocessor jar to filesystem at {}.", tmpLocation, e);
      if (tmpLocation.exists()) {
        LOG.info("Deleting partially copied coprocessor jar at {}.", tmpLocation);
        try {
          if (!tmpLocation.delete()) {
            LOG.error("Unable to delete partially copied coprocessor jar at {}.", tmpLocation, e);
          }
        } catch (IOException e1) {
          LOG.error("Unable to delete partially copied coprocessor jar at {}.", tmpLocation, e1);
          e.addSuppressed(e1);
        }
      }
      throw e;
    } finally {
      if (!jarFile.delete()) {
        LOG.warn("Unable to clean up temporary local coprocessor jar {}.", jarFile.getAbsolutePath());
      }
    }

    tmpLocation.renameTo(jarDependencyFile);
    return jarDependencyFile;
  }
}
