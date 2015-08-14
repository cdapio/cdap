/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;

/**
 * ClassLoader that filters out certain resources.
 */
public final class FilterClassLoader extends ClassLoader {

  private final Predicate<String> resourceAcceptor;
  private final Predicate<String> packageAcceptor;
  private final ClassLoader bootstrapClassLoader;

  public static FilterClassLoader create(ClassLoader parentClassLoader) {
    return create(null, parentClassLoader);
  }

  public static FilterClassLoader create(ProgramType programType, ClassLoader parentClassLoader) {
    Set<String> visibleResources = ProgramResources.getVisibleResources(parentClassLoader, programType);
    ImmutableSet.Builder<String> visiblePackages = ImmutableSet.builder();
    for (String resource : visibleResources) {
      if (resource.endsWith(".class")) {
        int idx = resource.lastIndexOf('/');
        // Ignore empty package
        if (idx > 0) {
          visiblePackages.add(resource.substring(0, idx));
        }
      }
    }
    return new FilterClassLoader(Predicates.in(visibleResources),
      Predicates.in(visiblePackages.build()), parentClassLoader);
  }

  public static FilterClassLoader create(Predicate<String> resourceAcceptor,
                                         Predicate<String> packageAcceptor, ClassLoader parentClassLoader) {
    return new FilterClassLoader(resourceAcceptor, packageAcceptor, parentClassLoader);
  }

  /**
   * @param resourceAcceptor Filter for accepting resources
   * @param parentClassLoader Parent classloader
   */
  private FilterClassLoader(Predicate<String> resourceAcceptor,
                           Predicate<String> packageAcceptor, ClassLoader parentClassLoader) {
    super(parentClassLoader);
    this.resourceAcceptor = resourceAcceptor;
    this.packageAcceptor = packageAcceptor;
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
  protected Package[] getPackages() {
    List<Package> packages = Lists.newArrayList();
    for (Package pkg : super.getPackages()) {
      if (packageAcceptor.apply(pkg.getName())) {
        packages.add(pkg);
      }
    }
    return packages.toArray(new Package[packages.size()]);
  }

  @Override
  protected Package getPackage(String name) {
    return (packageAcceptor.apply(name)) ? super.getPackage(name) : null;
  }

  @Override
  public URL getResource(String name) {
    URL resource = bootstrapClassLoader.getResource(name);
    if (resource != null) {
      return resource;
    }
    return resourceAcceptor.apply(name) ? super.getResource(name) : null;
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    Enumeration<URL> resources = bootstrapClassLoader.getResources(name);
    if (resources.hasMoreElements()) {
      return resources;
    }
    return resourceAcceptor.apply(name) ? super.getResources(name) : Collections.<URL>emptyEnumeration();
  }

  @Override
  public InputStream getResourceAsStream(String name) {
    InputStream resourceStream = bootstrapClassLoader.getResourceAsStream(name);
    if (resourceStream != null) {
      return resourceStream;
    }
    return resourceAcceptor.apply(name) ? super.getResourceAsStream(name) : null;
  }

  private String classNameToResourceName(String className) {
    return className.replace('.', '/') + ".class";
  }

  private boolean isValidResource(String resourceName) {
    return resourceAcceptor.apply(resourceName);
  }
}
