/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.program;

import com.continuuity.app.Id;
import com.google.common.base.Objects;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Locale;

/**
 * Factory helper to create {@link Program}.
 */
public final class Programs {

  public static Program createWithUnpack(Location location, File destinationUnpackedJarDir,
                                         ClassLoader parentClassLoader) throws IOException {
    return new DefaultProgram(location, destinationUnpackedJarDir, parentClassLoader);
  }

  public static Program createWithUnpack(Location location, File destinationUnpackedJarDir) throws IOException {
    return Programs.createWithUnpack(location, destinationUnpackedJarDir, getClassLoader());
  }

  /**
   * Creates a {@link Program} without expanding the location jar. The {@link Program#getClassLoader()}
   * would not function from the program this method returns.
   */
  public static Program create(Location location, ClassLoader classLoader) throws IOException {
    return new DefaultProgram(location, classLoader);
  }

  public static Program create(Location location) throws IOException {
    return new DefaultProgram(location, getClassLoader());
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
  public static Location programLocation(LocationFactory factory, String filePath, Id.Program id, Type type)
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
