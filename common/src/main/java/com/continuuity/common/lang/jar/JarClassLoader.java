/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.common.lang.jar;

import com.continuuity.common.lang.MultiClassLoader;

import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nullable;

/**
 * JarClassLoader extends {@link com.continuuity.common.lang.MultiClassLoader}.
 */
public class JarClassLoader extends MultiClassLoader {
  private Location jarLocation;
  private final JarResources jarResources;

  /**
   * Creates a ClassLoader that load classes from the given jar file.
   * @param jarLocation Location of the jar file
   * @throws IOException If there is error loading the jar file
   * @see #JarClassLoader(JarResources)
   */
  public JarClassLoader(Location jarLocation) throws IOException {
    this(new JarResources(jarLocation));
    this.jarLocation = jarLocation;
  }

  /**
   * Creates a ClassLoader with provided archive resources and uses context class loader as parent if available.
   * Otherwise, the class loader of this class would be used as parent class loader.
   * @param jarResources instance of archive resources
   */
  public JarClassLoader(JarResources jarResources) {
    this(jarResources,
         Thread.currentThread().getContextClassLoader() == null ?
           JarClassLoader.class.getClassLoader() : Thread.currentThread().getContextClassLoader());
  }

  /**
   * Creates a ClassLoader that load classes from the given jar file with the given ClassLoader as its parent.
   * @param jarLocation Location of the jar file.
   * @param parent Parent ClassLoader.
   * @throws IOException If there is error loading the jar file.
   */
  public JarClassLoader(Location jarLocation, ClassLoader parent) throws IOException {
    this(new JarResources(jarLocation), parent);
    this.jarLocation = jarLocation;
  }

  /**
   * Creates a ClassLoader with provided archive resources with the given ClassLoader as its parent.
   * @param jarResources instance of archive resources
   * @param parent Parent ClassLoader.
   */
  public JarClassLoader(JarResources jarResources, ClassLoader parent) {
    super(parent);
    this.jarResources = jarResources;
  }

  /**
   * Returns an input stream for reading the specified resource. If the resource is not found then it will try
   * finding it with its parent ClassLoader, if any.
   * @param s The resource name
   * @return An input stream for reading the resource, or null if the resource could not be found
   */
  @Override
  public InputStream getResourceAsStream(String s) {
    // Since entries in jarResources do not start with leading "/", remove it from s to query jarResources.
    String entry = s;
    if (s.startsWith("/")) {
      entry = entry.substring(1);
    }

    InputStream input;
    try {
      input = jarResources.getResourceAsStream(entry);
    } catch (IOException e) {
      input = null;
    }

    if (input == null) {
      ClassLoader parent = getParent();
      if (parent != null) {
        return parent.getResourceAsStream(s);
      }
    }
    return input;
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
    return jarResources.getResource(formatClassName(className));
  }

  @Nullable
  public Location getLocation() {
    return jarLocation;
  }
}
