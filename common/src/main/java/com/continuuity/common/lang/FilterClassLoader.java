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

package com.continuuity.common.lang;

import com.google.common.base.Predicate;
import sun.misc.CompoundEnumeration;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;

/**
 * ClassLoader that filters out certain resources.
 */
public class FilterClassLoader extends ClassLoader {

  private final Predicate<String> resourceAcceptor;

  private static final String[] STANDARD_LIB_PREFIXES = {
    "sun.org.",
    "com.sun.",
    "launcher.",
    "java.",
    "javax.",
    "org.ietf",
    "org.omg",
    "org.w3c",
    "org.xml",
    "sunw.",
    "sun.reflect.",
  };

  /**
   * @param resourceAcceptor Filter for accepting resources
   * @param parentClassLoader Parent classloader
   */
  public FilterClassLoader(Predicate<String> resourceAcceptor, ClassLoader parentClassLoader) {
    super(parentClassLoader);
    this.resourceAcceptor = resourceAcceptor;
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    if (isStandardLibraryClass(name) || isValidResource(classNameToResourceName(name))) {
      return super.loadClass(name, resolve);
    }

    throw new ClassNotFoundException(name);
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

  private boolean isStandardLibraryClass(String className) {
    for (String prefix : STANDARD_LIB_PREFIXES) {
      if (className.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  private String classNameToResourceName(String className) {
    return className.replace('.', '/') + ".class";
  }

  private boolean isValidResource(String resourceName) {
    return resourceAcceptor.apply(resourceName);
  }
}
