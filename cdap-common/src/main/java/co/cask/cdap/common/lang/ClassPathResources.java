/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.common.internal.guava.ClassPath;
import co.cask.cdap.common.internal.guava.ClassPath.ResourceInfo;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.internal.utils.Dependencies;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;
import java.util.Set;

/**
 * Utility methods for {@link ClassPath} {@link ResourceInfo resources}.
 */
public final class ClassPathResources {

  public static final Function<ClassPath.ClassInfo, String> CLASS_INFO_TO_CLASS_NAME =
    new Function<ClassPath.ClassInfo, String>() {
      @Override
      public String apply(ClassPath.ClassInfo input) {
        return input.getName();
      }
    };
  public static final Function<ClassPath.ResourceInfo, String> RESOURCE_INFO_TO_RESOURCE_NAME =
    new Function<ClassPath.ResourceInfo, String>() {
      @Override
      public String apply(ClassPath.ResourceInfo input) {
        return input.getResourceName();
      }
    };

  /**
   * Returns the base set of resources needed to load the specified {@link Class} using the
   * specified {@link ClassLoader}. Also traces and includes the dependencies for the specified class.
   *
   * @param classLoader the {@link ClassLoader} to use to generate the set of resources
   * @param classz the {@link Class} to generate the set of resources for
   * @return the set of resources needed to load the specified {@link Class} using the specified {@link ClassLoader}
   * @throws IOException
   */
  public static Set<String> getResourcesWithDependencies(ClassLoader classLoader, Class<?> classz) throws IOException {
    ClassPath classPath = getClassPath(classLoader, classz);

    // Add everything in the classpath as visible resources
    Set<String> result = Sets.newHashSet(Iterables.transform(classPath.getResources(),
                                                             RESOURCE_INFO_TO_RESOURCE_NAME));
    // Trace dependencies for all classes in the classpath
    findClassDependencies(
      classLoader, Iterables.transform(classPath.getAllClasses(), CLASS_INFO_TO_CLASS_NAME), result);

    return result;
  }

  /**
   * Returns a set of {@link ResourceInfo} required for the specified {@link Class} using the specified
   * {@link ClassLoader}. Does not trace dependencies of the specified class.
   *
   * @param classLoader the {@link ClassLoader} to use to return the set of {@link ResourceInfo} for the specified
   *                    {@link Class}
   * @param cls the {@link Class} for which to return the set of {@link ResourceInfo}
   * @return the set of {@link ResourceInfo} required for the specified {@link Class} using the specified
   * {@link ClassLoader}
   */
  public static Set<ClassPath.ResourceInfo> getClassPathResources(ClassLoader classLoader,
                                                                  Class<?> cls) throws IOException {
    return getClassPath(classLoader, cls).getResources();
  }

  /**
   * Returns a {@link ClassPath} instance that represents the classpath that the given class is loaded from the given
   * ClassLoader.
   */
  private static ClassPath getClassPath(ClassLoader classLoader, Class<?> cls) throws IOException {
    String resourceName = cls.getName().replace('.', '/') + ".class";
    URL url = classLoader.getResource(resourceName);
    if (url == null) {
      throw new IOException("Resource not found for " + resourceName);
    }

    try {
      URI classPathURI = getClassPathURL(resourceName, url).toURI();
      return ClassPath.from(classPathURI, classLoader);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  /**
   * Finds all resource names that the given set of classes depends on.
   *
   * @param classLoader class loader for looking up .class resources
   * @param classes set of class names that need to trace dependencies from
   * @param result collection to store the resulting resource names
   * @param <T> type of the result collection
   * @throws IOException if fails to load class bytecode during tracing
   */
  private static <T extends Collection<String>> T findClassDependencies(final ClassLoader classLoader,
                                                                        Iterable<String> classes,
                                                                        final T result) throws IOException {
    final Set<String> bootstrapClassPaths = getBootstrapClassPaths();
    final Set<URL> classPathSeen = Sets.newHashSet();

    Dependencies.findClassDependencies(classLoader, new ClassAcceptor() {
      @Override
      public boolean accept(String className, URL classUrl, URL classPathUrl) {
        // Ignore bootstrap classes
        if (bootstrapClassPaths.contains(classPathUrl.getFile())) {
          return false;
        }

        // Should ignore classes from SLF4J implementation, otherwise it will includes logback lib, which shouldn't be
        // visible through the program classloader.
        if (className.startsWith("org.slf4j.impl.")) {
          return false;
        }

        if (!classPathSeen.add(classPathUrl)) {
          return true;
        }

        // Add all resources in the given class path
        try {
          ClassPath classPath = ClassPath.from(classPathUrl.toURI(), classLoader);
          for (ClassPath.ResourceInfo resourceInfo : classPath.getResources()) {
            result.add(resourceInfo.getResourceName());
          }
        } catch (Exception e) {
          // If fail to get classes/resources from the classpath, ignore this classpath.
        }
        return true;
      }
    }, classes);

    return result;
  }

  /**
   * Returns a Set containing all bootstrap classpaths as defined in the {@code sun.boot.class.path} property.
   */
  private static Set<String> getBootstrapClassPaths() {
    // Get the bootstrap classpath. This is for exclusion while tracing class dependencies.
    Set<String> bootstrapPaths = Sets.newHashSet();
    for (String classpath : Splitter.on(File.pathSeparatorChar).split(System.getProperty("sun.boot.class.path"))) {
      File file = new File(classpath);
      bootstrapPaths.add(file.getAbsolutePath());
      try {
        bootstrapPaths.add(file.getCanonicalPath());
      } catch (IOException e) {
        // Ignore the exception and proceed.
      }
    }
    return bootstrapPaths;
  }

  /**
   * Find the URL of the classpath that contains the given resource.
   */
  private static URL getClassPathURL(String resourceName, URL resourceURL) {
    try {
      if ("file".equals(resourceURL.getProtocol())) {
        String path = resourceURL.getFile();
        // Compute the directory container the class.
        int endIdx = path.length() - resourceName.length();
        if (endIdx > 1) {
          // If it is not the root directory, return the end index to remove the trailing '/'.
          endIdx--;
        }
        return new URL("file", "", -1, path.substring(0, endIdx));
      }
      if ("jar".equals(resourceURL.getProtocol())) {
        String path = resourceURL.getFile();
        return URI.create(path.substring(0, path.indexOf("!/"))).toURL();
      }
    } catch (MalformedURLException e) {
      throw Throwables.propagate(e);
    }
    throw new IllegalStateException("Unsupported class URL: " + resourceURL);
  }

  private ClassPathResources() {
  }
}
