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

package io.cdap.cdap.hive;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.cdap.cdap.common.utils.DirUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * * Helper class for loading Hive classes.
 */
public final class ExploreUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ExploreUtils.class);
  private static final String EXPLORE_CLASSPATH = "explore.classpath";
  private static final String EXPLORE_CONF_DIRS = "explore.conf.dirs";

  private static final Function<String, File> STRING_FILE_FUNCTION = new Function<String, File>() {
    @Override
    public File apply(String input) {
      return new File(input).getAbsoluteFile();
    }
  };

  private static final Function<File, URL> FILE_TO_URL = new Function<File, URL>() {
    @Override
    public URL apply(File file) {
      try {
        return file.toURI().toURL();
      } catch (MalformedURLException e) {
        // This shouldn't happen
        throw Throwables.propagate(e);
      }
    }
  };

  private static ClassLoader exploreClassLoader;

  /**
   * Builds a class loader with the class path provided.
   */
  public static synchronized ClassLoader getExploreClassloader() {
    if (exploreClassLoader != null) {
      return exploreClassLoader;
    }

    // Use all hive jars and hive conf paths to construct the explore classloader
    List<URL> urls = new ArrayList<>();
    Iterables.addAll(urls, Iterables.transform(getExploreClasspathJarFiles(), FILE_TO_URL));
    Iterables.addAll(urls, Iterables.transform(getExploreConfDirs(), FILE_TO_URL));

    LOG.debug("Explore ClassLoader urls {}", urls);

    // The parent class loader is MainClassLoader since ExploreUtil will always be loaded from MainClassLoader
    exploreClassLoader = new URLClassLoader(urls.toArray(new URL[urls.size()]), ExploreUtils.class.getClassLoader());
    return exploreClassLoader;
  }

  /**
   * Returns the set of jar files used by hive. The set is constructed based on the system property
   * {@link #EXPLORE_CLASSPATH}. The {@link #EXPLORE_CLASSPATH} is expected to contains one or more file paths,
   * separated by the {@link File#pathSeparatorChar}. Only jar files will be included in the result set and paths
   * ended with a '*' will be expanded to include all jars under the given path.
   *
   * @param extraExtensions if provided, the set of file extensions that is also accepted when resolving the
   *                        classpath wildcard
   *
   * @throws IllegalArgumentException if the system property {@link #EXPLORE_CLASSPATH} is missing.
   */
  public static Iterable<File> getExploreClasspathJarFiles(String...extraExtensions) {
    String property = System.getProperty(EXPLORE_CLASSPATH);
    if (property == null) {
      throw new RuntimeException("System property " + EXPLORE_CLASSPATH + " is not set.");
    }

    Iterable<File> classpathJarFiles = getClasspathJarFiles(property, extraExtensions);
    LOG.trace("Explore classpath jar files: {}", classpathJarFiles);
    return classpathJarFiles;
  }

  /**
   * Returns the set of jars in Java classpath. Only jar files will be included in the result set and paths
   * ended with a '*' will be expanded to include all jars under the given path.
   *
   * @param classpath java classpath separated by the {@link File#pathSeparatorChar}
   * @param extraExtensions if provided, the set of file extensions that is also accepted when resolving the
   *                        classpath wildcard
   */
  public static Iterable<File> getClasspathJarFiles(String classpath, String...extraExtensions) {
    Set<String> acceptedExts = Sets.newHashSet(extraExtensions);
    acceptedExts.add("jar");

    Set<File> result = new LinkedHashSet<>();
    for (String path : Splitter.on(File.pathSeparator).split(classpath)) {
      List<File> jarFiles;
      // The path has to either ends with "*" or is a jar file. This is because we are only interested in JAR files
      // in the hive classpath.
      if (path.endsWith("*")) {
        jarFiles = DirUtils.listFiles(new File(path.substring(0, path.length() - 1)).getAbsoluteFile(), acceptedExts);
      } else if (path.endsWith(".jar")) {
        jarFiles = Collections.singletonList(new File(path));
      } else {
        continue;
      }

      // Resolves all files to the actual file to remove symlinks that point to the same file.
      // Also, only add files that are readable
      for (File jarFile : jarFiles) {
        try {
          Path jarPath = jarFile.toPath().toRealPath();
          if (Files.isRegularFile(jarPath) && Files.isReadable(jarPath)) {
            result.add(jarPath.toFile());
          }
        } catch (IOException e) {
          LOG.debug("Ignore jar file that is not readable {}", jarFile);
        }
      }
    }

    return Collections.unmodifiableSet(result);
  }

  /**
   * Returns the set of config files used by hive. The set is constructed based on the system property
   * {@link #EXPLORE_CONF_DIRS}. The {@link #EXPLORE_CONF_DIRS} is expected to contains one or more file paths,
   * separated by the {@link File#pathSeparatorChar}. If a given path is a directory, the immediate files under that
   * directory will be included in the result set instead of the directory itself.
   *
   * @throws IllegalArgumentException if the system property {@link #EXPLORE_CONF_DIRS} is missing.
   */
  public static Iterable<File> getExploreConfFiles() {
    Set<File> result = new LinkedHashSet<>();
    for (File confPath : getExploreConfDirs()) {
      if (confPath.isDirectory()) {
        result.addAll(DirUtils.listFiles(confPath));
      } else if (confPath.isFile()) {
        result.add(confPath);
      }
    }

    return Collections.unmodifiableSet(result);
  }

  /**
   * Returns the conf file paths based on the {@link #EXPLORE_CONF_DIRS} system property. It simply splits the
   * property with {@link File#separatorChar} and returns the set of {@link File} representing the paths.
   *
   * @throws IllegalArgumentException if the system property {@link #EXPLORE_CONF_DIRS} is missing.
   */
  private static Iterable<File> getExploreConfDirs() {
    String property = System.getProperty(EXPLORE_CONF_DIRS);
    if (property == null) {
      throw new IllegalArgumentException("System property " + EXPLORE_CONF_DIRS + " is not set.");
    }
    return Iterables.transform(Splitter.on(File.pathSeparator).split(property), STRING_FILE_FUNCTION);
  }
}
