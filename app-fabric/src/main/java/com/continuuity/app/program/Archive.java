/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.program;

import com.continuuity.app.Id;
import com.continuuity.archive.JarClassLoader;
import com.continuuity.archive.JarResources;
import com.continuuity.internal.UserErrors;
import com.continuuity.internal.UserMessages;
import com.continuuity.weave.filesystem.Location;

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
  private final ClassLoader jarClassLoader;
  private final String mainClassName;
  private final Id.Account id;

  public Archive(Id.Account id, Location location) throws IOException {
    this(id, new JarResources(location));
  }

  private Archive(Id.Account id, JarResources jarResources) throws IOException {
    jarClassLoader = new JarClassLoader(jarResources);
    this.id = id;

    Manifest manifest = jarResources.getManifest();
    check(manifest != null, UserMessages.getMessage(UserErrors.BAD_JAR_MANIFEST));

    mainClassName = manifest.getMainAttributes().getValue(ManifestFields.MAIN_CLASS);
    check(mainClassName != null, UserMessages.getMessage(UserErrors.BAD_JAR_ATTRIBUTE), ManifestFields.MAIN_CLASS);
  }

  public Class<?> getMainClass() throws ClassNotFoundException {
    return jarClassLoader.loadClass(mainClassName);
  }

  private void check(boolean condition, String fmt, Object... objs) throws IOException {
    if (!condition) {
      throw new IOException(String.format(fmt, objs));
    }
  }
}
