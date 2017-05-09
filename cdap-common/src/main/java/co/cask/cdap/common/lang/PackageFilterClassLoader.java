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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A {@link ClassLoader} that filter class based on package name. Classes in the bootstrap ClassLoader is always
 * loadable from this ClassLoader.
 */
public class PackageFilterClassLoader extends ClassLoader {

  private final Predicate<String> predicate;
  private final ClassLoader bootstrapClassLoader;

  /**
   * Constructs a new instance that only allow class's package name passes the given {@link Predicate}.
   */
  public PackageFilterClassLoader(ClassLoader parent, Predicate<String> predicate) {
    super(parent);
    this.predicate = predicate;
    // There is no reliable way to get bootstrap ClassLoader from Java (System.class.getClassLoader() may return null).
    // A URLClassLoader with no URLs and with a null parent will load class from bootstrap ClassLoader only.
    this.bootstrapClassLoader = new URLClassLoader(new URL[0], null);
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    try {
      return bootstrapClassLoader.loadClass(name);
    } catch (ClassNotFoundException e) {
      if (!predicate.apply(getClassPackage(name))) {
        throw new ClassNotFoundException("Loading of class " + name + " not allowed");
      }

      return super.loadClass(name, resolve);
    }
  }

  @Override
  public URL getResource(String name) {
    URL resource = bootstrapClassLoader.getResource(name);
    if (resource != null) {
      return resource;
    }

    if (name.endsWith(".class") && !predicate.apply(getResourcePackage(name))) {
      return null;
    }
    return super.getResource(name);
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    Enumeration<URL> resources = bootstrapClassLoader.getResources(name);
    if (resources.hasMoreElements()) {
      return resources;
    }
    if (name.endsWith(".class") && !predicate.apply(getResourcePackage(name))) {
      return Iterators.asEnumeration(Iterators.<URL>emptyIterator());
    }
    return super.getResources(name);
  }

  @Override
  protected Package[] getPackages() {
    List<Package> packages = Lists.newArrayList();
    for (Package pkg : super.getPackages()) {
      if (predicate.apply(pkg.getName())) {
        packages.add(pkg);
      }
    }
    return packages.toArray(new Package[packages.size()]);
  }

  @Override
  protected Package getPackage(String name) {
    if (!predicate.apply(name)) {
      return null;
    }
    return super.getPackage(name);
  }

  /**
   * Returns the package of the given class or {@code null} if the class is in default package.
   *
   * @param className fully qualified name of the class
   */
  @Nullable
  private String getClassPackage(String className) {
    int idx = className.lastIndexOf('.');
    return idx < 0 ? null : className.substring(0, idx);
  }

  /**
   * Returns the package name of the given resource name representing a class.
   *
   * @param classResource Resource name of the class.
   */
  private String getResourcePackage(String classResource) {
    String packageName = classResource.substring(0, classResource.length() - ".class".length()).replace('/', '.');
    if (packageName.startsWith("/")) {
      return packageName.substring(1);
    }
    return packageName;
  }
}
