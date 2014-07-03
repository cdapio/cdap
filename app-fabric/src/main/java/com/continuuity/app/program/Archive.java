/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.program;

import com.continuuity.api.Application;
import com.continuuity.common.lang.ApiResourceListHolder;
import com.continuuity.common.lang.ClassLoaders;
import com.google.common.base.Objects;

import java.io.File;
import java.io.IOException;

/**
 * Represents the archive that is uploaded by the user using the deployment
 * service.
 */
public final class Archive {
  /**
   * Class loader for holding.
   */
  private final ClassLoader classLoader;
  private final String mainClassName;

  public Archive(File unpackedJarFolder, String mainClassName) throws IOException {
    this.classLoader = ClassLoaders.newProgramClassLoader(
      unpackedJarFolder, ApiResourceListHolder.getResourceList(),
      Objects.firstNonNull(Thread.currentThread().getContextClassLoader(), Archive.class.getClassLoader()));
    this.mainClassName = mainClassName;
  }

  @SuppressWarnings("unchecked")
  public Class<Application> getMainClass() throws ClassNotFoundException {
    return (Class<Application>) classLoader.loadClass(mainClassName);
  }
}
