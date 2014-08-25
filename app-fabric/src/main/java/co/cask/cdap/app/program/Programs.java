/*
 * Copyright 2014 Cask, Inc.
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

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Objects;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Locale;

/**
 * Factory helper to create {@link Program}.
 */
public final class Programs {

  /**
   * Creates a {@link co.cask.cdap.app.program.Program} with the supplied list of dataset jar files and
   * program jar file.
   */
  public static Program createWithUnpack(Location location, List<Location> datasetTypeJars,
                                         File destinationUnpackedJarDir) throws IOException {
    return new DefaultProgram(location, datasetTypeJars, destinationUnpackedJarDir, getClassLoader());
  }

  /**
   * Creates a {@link Program} without expanding the location jar. The {@link Program#getClassLoader()}
   * would not function from the program this method returns.
   */
  public static Program create(Location location, List<Location> datasetTypeJars,
                               ClassLoader classLoader) throws IOException {
    return new DefaultProgram(location, datasetTypeJars, classLoader);
  }

  public static Program create(Location location, List<Location> datasetJars) throws IOException {
    return new DefaultProgram(location, datasetJars, getClassLoader());
  }

  /**
   * Get program location
   *
   * @param factory  location factory
   * @param filePath app fabric output directory path
   * @param id       program id
   * @param type     type of the program
   * @return         Location corresponding to the program id
   * @throws IOException incase of errors
   */
  public static Location programLocation(LocationFactory factory, String filePath, Id.Program id, ProgramType type)
    throws IOException {
    Location allAppsLocation = factory.create(filePath);

    Location accountAppsLocation = allAppsLocation.append(id.getAccountId());
    String name = String.format(Locale.ENGLISH, "%s/%s", type.toString(), id.getApplicationId());
    Location applicationProgramsLocation = accountAppsLocation.append(name);
    if (!applicationProgramsLocation.exists()) {
      throw new FileNotFoundException("Unable to locate the Program,  location doesn't exist: "
                                        + applicationProgramsLocation.toURI().getPath());
    }
    Location programLocation = applicationProgramsLocation.append(String.format("%s.jar", id.getId()));
    if (!programLocation.exists()) {
      throw new FileNotFoundException(String.format("Program %s.%s of type %s does not exists.",
                                                    id.getApplication(), id.getId(), type));
    }
    return programLocation;
  }

  private static ClassLoader getClassLoader() {
    return Objects.firstNonNull(Thread.currentThread().getContextClassLoader(), Programs.class.getClassLoader());
  }

  private Programs() {
  }
}
