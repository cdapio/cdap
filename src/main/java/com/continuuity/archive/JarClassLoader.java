/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.archive;

import com.continuuity.filesystem.Location;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * JarClassLoader extends {@link MultiClassLoader}
 */
public class JarClassLoader extends MultiClassLoader {
  private final JarResources jarResources;

  /**
   * Create the JarResource and suck in the archive file.
   *
   * @param jarName Name of the archive file.
   */
  public JarClassLoader(String jarName) throws IOException {
    jarResources = new JarResources(jarName);
  }

  public JarClassLoader(Location jarName) throws IOException {
    jarResources = new JarResources(jarName);
  }

  /**
   * Creates an instance of archive with provided archive resources.
   *
   * @param jarResources instance of archive resources
   */
  public JarClassLoader(JarResources jarResources) {
    this.jarResources = jarResources;
  }

  /**
   * Loads the class bytes based on the name specified. Name
   * munging is used to identify the class to be loaded from
   * the archive.
   *
   * @param className Name of the class bytes to be loaded.
   * @return array of bytes for the class.
   */
  @Override
  @Nullable
  public byte[] loadClassBytes(String className) {
    className = formatClassName(className);
    return ( jarResources.getResource(className) );
  }
}
