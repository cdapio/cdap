/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.common.lang;

import com.google.common.base.Predicate;
import sun.misc.CompoundEnumeration;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;

/**
 * ClassLoader that filters out certain resources.
 */
public class FilterClassLoader extends ClassLoader {

  private final Predicate<String> resourceAcceptor;
  private final ClassLoader bootstrapClassLoader;

  /**
   * @param resourceAcceptor Filter for accepting resources
   * @param parentClassLoader Parent classloader
   */
  public FilterClassLoader(Predicate<String> resourceAcceptor, ClassLoader parentClassLoader) {
    super(parentClassLoader);
    this.resourceAcceptor = resourceAcceptor;
    this.bootstrapClassLoader = new URLClassLoader(new URL[0], null);
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    // Try to load it from bootstrap class loader first
    try {
      return bootstrapClassLoader.loadClass(name);
    } catch (ClassNotFoundException e) {
      if (isValidResource(classNameToResourceName(name))) {
        return super.loadClass(name, resolve);
      }
      throw e;
    }
  }

  @Override
  public URL findResource(String name) {
    if (isValidResource(name)) {
      return super.findResource(name);
    }

    return null;
  }

  @Override
  public Enumeration<URL> findResources(String name) throws IOException {
    if (isValidResource(name)) {
      return super.findResources(name);
    }

    return new CompoundEnumeration<URL>(new Enumeration[0]);
  }

  private String classNameToResourceName(String className) {
    return className.replace('.', '/') + ".class";
  }

  private boolean isValidResource(String resourceName) {
    return resourceAcceptor.apply(resourceName);
  }
}
