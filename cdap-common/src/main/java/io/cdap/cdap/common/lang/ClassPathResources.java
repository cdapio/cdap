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

package io.cdap.cdap.common.lang;

import io.cdap.cdap.common.internal.guava.ClassPath;
import io.cdap.cdap.common.internal.guava.ClassPath.ClassInfo;
import io.cdap.cdap.common.internal.guava.ClassPath.ResourceInfo;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.internal.utils.Dependencies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for {@link ClassPath} {@link ResourceInfo resources}.
 */
public final class ClassPathResources {

  private static final Logger LOG = LoggerFactory.getLogger(ClassPathResources.class);

  /**
   * Return a list of all URL entries from the java class path.
   *
   * @return list of all URL entries from the java class path.
   */
  public static List<URL> getClasspathUrls() {
    List<URL> urls = new ArrayList<>();

    // wildcards are expanded before they are places in java.class.path
    // for example, java -cp lib/* will list out all the jars in the lib directory and
    // add each individual jars as an element in java.class.path. The exception is if
    // the directory doesn't exist, then it will be preserved as lib/*.
    for (String path : System.getProperty("java.class.path").split(File.pathSeparator)) {
      try {
        urls.add(Paths.get(path).toRealPath().toUri().toURL());
      } catch (NoSuchFileException e) {
        // ignore anything that doesn't exist
      } catch (MalformedURLException e) {
        // should never happen
        throw new RuntimeException(e);
      } catch (IOException e) {
        throw new RuntimeException("Unable to get class path entry " + path, e);
      }
    }
    return urls;
  }

  /**
   * Returns the base set of resources needed to load the specified {@link Class} using the
   * specified {@link ClassLoader}. Also traces and includes the dependencies for the specified
   * class.
   *
   * @param classLoader the {@link ClassLoader} to use to generate the set of resources
   * @param classz the {@link Class} to generate the set of resources for
   * @return the set of resources needed to load the specified {@link Class} using the specified
   *     {@link ClassLoader}
   */
  public static Set<String> getResourcesWithDependencies(ClassLoader classLoader, Class<?> classz)
      throws IOException {
    ClassPath classPath = getClassPath(classLoader, classz);

    // Add everything in the classpath as visible resources
    Set<String> result = classPath.getResources().stream()
        .map(ResourceInfo::getResourceName)
        .collect(Collectors.toSet());
    // Trace dependencies for all classes in the classpath
    return findClassDependencies(
        classLoader,
        classPath.getAllClasses().stream().map(ClassInfo::getName).collect(Collectors.toList()),
        result);
  }

  /**
   * Returns a set of {@link ResourceInfo} required for the specified {@link Class} using the
   * specified {@link ClassLoader}. Does not trace dependencies of the specified class.
   *
   * @param classLoader the {@link ClassLoader} to use to return the set of {@link ResourceInfo}
   *     for the specified {@link Class}
   * @param cls the {@link Class} for which to return the set of {@link ResourceInfo}
   * @return the set of {@link ResourceInfo} required for the specified {@link Class} using the
   *     specified {@link ClassLoader}
   */
  public static Set<ClassPath.ResourceInfo> getClassPathResources(ClassLoader classLoader,
      Class<?> cls) throws IOException {
    return getClassPath(classLoader, cls).getResources();
  }

  /**
   * Returns a {@link ClassPath} instance that represents the classpath that the given class is
   * loaded from the given ClassLoader.
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
   * Returns a {@link URLClassLoader} for loading classes provided the JVM platform.
   */
  private static URLClassLoader getPlatformClassLoader() {
    ClassLoader platformClassloader;
    try {
      // For Java11+, there is a ClassLoader.getPlatformClassLoader() method to get
      // the platform classloader.
      //noinspection JavaReflectionMemberAccess
      platformClassloader = (ClassLoader) ClassLoader.class.getMethod("getPlatformClassLoader")
          .invoke(null);
    } catch (Exception e) {
      // For Java8, the parent of the system classloader is the
      // platform classloader (bootstrap + ext classloader)
      platformClassloader = ClassLoader.getSystemClassLoader().getParent();
    }
    // Always warp it with a URLClassLoader to simplify handling of
    // the potential `null` parent classloader in the Java 8 case.
    return new URLClassLoader(new URL[0], platformClassloader);
  }

  static ClassAcceptor createClassAcceptor(ClassLoader classLoader, Collection<String> result)
      throws IOException {
    try (URLClassLoader platformClassloader = getPlatformClassLoader()) {
      Set<URL> classPathSeen = new HashSet<>();
      return new ClassAcceptor() {
        @Override
        public boolean accept(String className, URL classUrl, URL classPathUrl) {

          // Ignore platform classes
          if (platformClassloader.getResource(className.replace('.', '/') + ".class") != null) {
            return false;
          }

          // Should ignore classes from SLF4J implementation, otherwise it will include logback lib,
          // which shouldn't be visible through the program classloader.
          if (className.startsWith("org.slf4j.impl.")) {
            return false;
          }

          // Ignore classes with incompatible Java specification version in multi-release jars.
          // See https://docs.oracle.com/javase/10/docs/specs/jar/jar.html#multi-release-jar-files
          // for details.
          if (className.startsWith("META-INF.versions.")) {
            // Get current Java specification version
            // See https://docs.oracle.com/en/java/javase/12/docs/api/java.base/java/lang/System.html#getProperties().
            String javaSpecVersion = System.getProperty("java.specification.version")
                .replaceFirst("^1[.]", "");
            int version;
            try {
              version = Integer.parseInt(javaSpecVersion);
            } catch (NumberFormatException e) {
              throw new IllegalStateException(
                  String.format("Failed to parse Java specification version string %s",
                      javaSpecVersion), e);
            }
            if (version < 9) {
              // Per JAR spec, classes for Java 8 and below will not be versioned.
              return false;
            }
            // Check if the specification version matches the dependency version
            try {
              int classVersion = Integer.parseInt(className.split("[.]")[2]);
              if (version < classVersion) {
                return false;
              }
            } catch (NumberFormatException e) {
              // If the class version fails to parse, allow it through.
              LOG.debug("Failed to parse multi-release versioned dependency class '{}'", className);
            }
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
      };
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
  private static <T extends Collection<String>> T findClassDependencies(
      final ClassLoader classLoader,
      Iterable<String> classes,
      final T result) throws IOException {
    Dependencies.findClassDependencies(classLoader, createClassAcceptor(classLoader, result),
        classes);
    return result;
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
      throw new RuntimeException(e);
    }
    throw new IllegalStateException("Unsupported class URL: " + resourceURL);
  }

  private ClassPathResources() {
  }
}
