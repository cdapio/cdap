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
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.List;

/**
 * ClassLoader that implements bundle jar feature, in which the application jar contains
 * its dependency jars inside.
 */
public class ProgramClassLoader extends URLClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramClassLoader.class);
  private static final FilenameFilter JAR_FILE_FILTER = new FilenameFilter() {
    @Override
    public boolean accept(File dir, String name) {
      return name.endsWith(".jar");
    }
  };

  /**
   * Constructs an instance that load classes from the given directory. The URLs for class loading are:
   * <p/>
   * <pre>
   * [dir]
   * [dir]/*.jar
   * [dir]/lib/*.jar
   * </pre>
   */
  public static ProgramClassLoader create(File unpackedJarDir, ClassLoader parentClassLoader) throws IOException {
    Predicate<String> predicate = Predicates.in(ProgramResources.getVisibleResources());
    ClassLoader filteredParent = new FilterClassLoader(predicate, parentClassLoader);
    return new ProgramClassLoader(unpackedJarDir, filteredParent);
  }

  private ProgramClassLoader(File dir, ClassLoader parent) {
    super(getClassPathUrls(dir), parent);
  }

  private static URL[] getClassPathUrls(File dir) {
    try {
      List<URL> urls = Lists.newArrayList(dir.toURI().toURL());
      addJarURLs(dir, urls);
      addJarURLs(new File(dir, "lib"), urls);
      return urls.toArray(new URL[urls.size()]);
    } catch (MalformedURLException e) {
      // Should never happen
      LOG.error("Error in adding jar URLs to classPathUrls", e);
      throw Throwables.propagate(e);
    }
  }

  private static void addJarURLs(File dir, List<URL> result) throws MalformedURLException {
    File[] files = dir.listFiles(JAR_FILE_FILTER);
    if (files == null) {
      return;
    }

    for (File file : files) {
      result.add(file.toURI().toURL());
    }
  }

  /**
   * ClassLoader that filters out certain resources.
   */
  private static final class FilterClassLoader extends ClassLoader {

    private final Predicate<String> resourceAcceptor;
    private final ClassLoader bootstrapClassLoader;

    /**
     * @param resourceAcceptor Filter for accepting resources
     * @param parentClassLoader Parent classloader
     */
    FilterClassLoader(Predicate<String> resourceAcceptor, ClassLoader parentClassLoader) {
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

      return Iterators.asEnumeration(ImmutableList.<URL>of().iterator());
    }

    private String classNameToResourceName(String className) {
      return className.replace('.', '/') + ".class";
    }

    private boolean isValidResource(String resourceName) {
      return resourceAcceptor.apply(resourceName);
    }
  }
}
