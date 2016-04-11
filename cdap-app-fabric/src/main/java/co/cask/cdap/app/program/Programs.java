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

import co.cask.cdap.app.runtime.ProgramClassLoaderFilterProvider;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.lang.FilterClassLoader;
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.internal.app.program.ForwardingProgram;
import co.cask.cdap.proto.Id;
import com.google.common.base.Objects;
import com.google.common.io.Closeables;
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
   * Creates {@link Program} that can be executed by the given {@link ProgramRunner}.
   *
   * @param cConf the CDAP configuration
   * @param programRunner the {@link ProgramRunner} for executing the program
   * @param programJarLocation the {@link Location} of the program jar file
   * @param unpackedDir a directory that the program jar file was unpacked to
   * @return a new {@link Program} instance.
   * @throws IOException If failed to create the program
   */
  public static Program create(CConfiguration cConf, ProgramRunner programRunner,
                               Location programJarLocation, File unpackedDir) throws IOException {
    FilterClassLoader.Filter filter;
    if (programRunner instanceof ProgramClassLoaderFilterProvider) {
      filter = ((ProgramClassLoaderFilterProvider) programRunner).getFilter();
    } else {
      filter = FilterClassLoader.defaultFilter();
    }

    if (filter == null) {
      // Shouldn't happen. This is to catch invalid ProgramClassLoaderFilterProvider implementation
      // since it's provided by the ProgramRunner, which can be external to CDAP
      throw new IOException("Program classloader filter cannot be null");
    }

    FilterClassLoader parentClassLoader = new FilterClassLoader(programRunner.getClass().getClassLoader(), filter);
    final ProgramClassLoader programClassLoader = new ProgramClassLoader(cConf, unpackedDir, parentClassLoader);

    return new ForwardingProgram(Programs.create(programJarLocation, programClassLoader)) {
      @Override
      public void close() throws IOException {
        Closeables.closeQuietly(programClassLoader);
        super.close();
      }
    };
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
