/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.program;

import com.continuuity.app.Id;
import com.continuuity.common.lang.jar.JarResources;
import com.continuuity.common.lang.jar.ProgramClassLoader;
import com.continuuity.common.lang.jar.ProgramJarResources;
import com.continuuity.internal.UserErrors;
import com.continuuity.internal.UserMessages;

import java.io.File;
import java.io.IOException;
import java.util.jar.Manifest;

/**
 * Represents the archive that is uploaded by the user using the deployment
 * service.
 */
public final class Archive {
  /**
   * Class loader for holding
   */
  private final ClassLoader classLoader;
  private final String mainClassName;
  private final Id.Account id;

  public Archive(Id.Account id, File unpackedJarFolder) throws IOException {
    classLoader = new ProgramClassLoader(unpackedJarFolder);
    this.id = id;

    ProgramJarResources jarResources = new ProgramJarResources(unpackedJarFolder);
    Manifest manifest = jarResources.getManifest();
    check(manifest != null, UserMessages.getMessage(UserErrors.BAD_JAR_MANIFEST));

    mainClassName = manifest.getMainAttributes().getValue(ManifestFields.MAIN_CLASS);
    check(mainClassName != null, UserMessages.getMessage(UserErrors.BAD_JAR_ATTRIBUTE), ManifestFields.MAIN_CLASS);
  }

  public Class<?> getMainClass() throws ClassNotFoundException {
    return classLoader.loadClass(mainClassName);
  }

  private void check(boolean condition, String fmt, Object... objs) throws IOException {
    if (!condition) {
      throw new IOException(String.format(fmt, objs));
    }
  }
}
