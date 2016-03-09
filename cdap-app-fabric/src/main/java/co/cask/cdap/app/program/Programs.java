/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
package co.cask.cdap.app.program;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.proto.Id;
import com.google.common.base.Objects;
import org.apache.twill.filesystem.Location;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Factory helper to create {@link Program}.
 */
public final class Programs {

  /**
   * Creates a {@link Program} by expanding the location jar into a given dir. The unpacked directory
   * is used to create the program class loader.
   */
  public static Program createWithUnpack(CConfiguration cConf, Location location,
                                         File destinationUnpackedJarDir) throws IOException {
    return new DefaultProgram(location, cConf, destinationUnpackedJarDir, getClassLoader());
  }

  /**
   * Creates a {@link Program} without expanding the location jar. The {@link Program#getClassLoader()}
   * will be the given ClassLoader.
   */
  public static Program create(Location location, ClassLoader classLoader) throws IOException {
    return new DefaultProgram(location, classLoader);
  }

  /**
   * Creates a {@link Program} without expanding the location jar. The {@link Program#getClassLoader()}
   * will be the context class loader or cdap system ClassLoader.
   */
  public static Program create(Location location) throws IOException {
    return new DefaultProgram(location, getClassLoader());
  }

  /**
   * Get program location
   *
   * @param namespacedLocationFactory the namespaced location on the file system
   * @param appFabricDir app fabric output directory path
   * @param id program id
   * @return Location corresponding to the program id
   * @throws IOException incase of errors
   */
  public static Location programLocation(NamespacedLocationFactory namespacedLocationFactory, String appFabricDir,
                                         Id.Program id) throws IOException {
    Location namespaceHome = namespacedLocationFactory.get(id.getNamespace());
    if (!namespaceHome.exists()) {
      throw new FileNotFoundException("Unable to locate the Program, namespace location doesn't exist: " +
                                        namespaceHome);
    }
    Location appFabricLocation = namespaceHome.append(appFabricDir);

    Location applicationProgramsLocation =
      appFabricLocation.append(id.getApplicationId()).append(id.getType().toString());
    if (!applicationProgramsLocation.exists()) {
      throw new FileNotFoundException("Unable to locate the Program,  location doesn't exist: " +
                                        applicationProgramsLocation);
    }
    Location programLocation = applicationProgramsLocation.append(String.format("%s.jar", id.getId()));
    if (!programLocation.exists()) {
      throw new FileNotFoundException(String.format("Program %s.%s of type %s does not exists.",
                                               id.getApplication(), id.getId(), id.getType()));
    }
    return programLocation;
  }

  private static ClassLoader getClassLoader() {
    return Objects.firstNonNull(Thread.currentThread().getContextClassLoader(), Programs.class.getClassLoader());
  }

  private Programs() {
  }
}
