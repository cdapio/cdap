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

package io.cdap.cdap.runtime.runtimejob;

import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.LocalFile;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.ApplicationBundler;
import org.apache.twill.internal.DefaultLocalFile;

import java.io.IOException;

/**
 *
 */
public final class DatarpocJarUtils {

  /**
   *
   * @param bundler
   * @param locationFactory
   * @param name
   * @param classes
   * @return
   * @throws IOException
   */
  public static LocalFile getBundleJar(ApplicationBundler bundler, LocationFactory locationFactory, String name,
                                       Iterable<Class<?>> classes) throws IOException {
    Location targetLocation = locationFactory.create(name);
    bundler.createBundle(targetLocation, classes);
    return createLocalFile(name, targetLocation, true);
  }

  /**
   *
   * @return
   */
  public static ApplicationBundler createBundler() {
    return new ApplicationBundler(new ClassAcceptor());
  }

  private static LocalFile createLocalFile(String name, Location location, boolean archive) throws IOException {
    return new DefaultLocalFile(name, location.toURI(), location.lastModified(), location.length(), archive, null);
  }

  private DatarpocJarUtils() {
    // no-op
  }
}
