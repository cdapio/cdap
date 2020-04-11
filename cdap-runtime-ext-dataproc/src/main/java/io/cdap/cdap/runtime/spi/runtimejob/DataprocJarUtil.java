/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.runtimejob;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import joptsimple.OptionSpec;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.LocalFile;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.ApplicationBundler;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.DefaultLocalFile;
import org.apache.twill.internal.appmaster.ApplicationMasterMain;
import org.apache.twill.internal.container.TwillContainerMain;
import org.apache.twill.internal.utils.Dependencies;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * Util class to build jar files needed by {@code DataprocRuntimeJobManager}.
 */
final class DataprocJarUtil {

  /**
   * Returns twill bundle jar.
   *
   * @param locationFactory location factory to create location
   * @return a runtime jar file
   * @throws IOException any error while building the jar
   */
  static LocalFile getTwillJar(LocationFactory locationFactory) throws IOException {
    ApplicationBundler bundler = new ApplicationBundler(new ClassAcceptor() {
      @Override
      public boolean accept(String className, URL classUrl, URL classPathUrl) {
        return !className.startsWith("org.apache.hadoop") && !classPathUrl.toString().contains("spark-assembly");
      }
    });
    Location location = locationFactory.create(Constants.Files.TWILL_JAR);
    bundler.createBundle(location, ImmutableList.of(ApplicationMasterMain.class,
                                                    TwillContainerMain.class, OptionSpec.class));
    return createLocalFile(location, true);
  }

  /**
   * Returns a thin launcher jar containing classes that are needed for {@code DataprocJobMain}.
   *
   * @param locationFactory location factory to create location
   * @return a runtime jar file
   * @throws IOException any error while building the jar
   */
  static LocalFile getLauncherJar(LocationFactory locationFactory) throws IOException {
    Location location = locationFactory.create("launcher.jar");
    try (JarOutputStream jarOut = new JarOutputStream(location.getOutputStream())) {
      ClassLoader classLoader = DataprocRuntimeJobManager.class.getClassLoader();
      Dependencies.findClassDependencies(classLoader, new ClassAcceptor() {
        @Override
        public boolean accept(String className, URL classUrl, URL classPathUrl) {
          if (className.startsWith("io.cdap.cdap.runtime")) {
            try {
              jarOut.putNextEntry(new JarEntry(className.replace('.', '/') + ".class"));
              try (InputStream is = classUrl.openStream()) {
                ByteStreams.copy(is, jarOut);
              }
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            return true;
          }
          return false;
        }
      }, DataprocJobMain.class.getName());
    }
    return createLocalFile(location, false);
  }

  private static LocalFile createLocalFile(Location location, boolean archive) throws IOException {
    return new DefaultLocalFile(location.getName(), location.toURI(),
                                location.lastModified(), location.length(), archive, null);
  }

  private DataprocJarUtil() {
    // no-op
  }
}
