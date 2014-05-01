/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.program;

import com.continuuity.app.Id;
import com.continuuity.common.lang.jar.BundleJarUtil;
import com.continuuity.common.lang.jar.JarResources;
import com.continuuity.common.lang.jar.ProgramJarResources;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import org.apache.twill.filesystem.LocalLocationFactory;
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

  public static Program create(Location location, File destinationUnpackedJarDir,
                               ClassLoader parentClassLoader) throws IOException {
    Preconditions.checkArgument(location != null);
    Preconditions.checkArgument(location.exists());
    Preconditions.checkArgument(destinationUnpackedJarDir != null);
    Preconditions.checkArgument(destinationUnpackedJarDir.exists());

    File unpackedJarDir = BundleJarUtil.unpackProgramJar(location, destinationUnpackedJarDir);
    return new DefaultProgram(location, unpackedJarDir, new ProgramJarResources(unpackedJarDir), parentClassLoader);
  }

  public static Program create(Location location, File destinationUnpackedJarDir) throws IOException {
    return Programs.create(location, destinationUnpackedJarDir, null);
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

  private Programs() {
  }
}
