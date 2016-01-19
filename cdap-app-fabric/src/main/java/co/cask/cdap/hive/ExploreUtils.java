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

package co.cask.cdap.hive;

import co.cask.cdap.common.conf.Constants;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * * Helper class for loading Hive classes.
 */
public final class ExploreUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ExploreUtils.class);
  private static ClassLoader exploreClassLoader = null;

  /**
   * Builds a class loader with the class path provided.
   */
  public static synchronized ClassLoader getExploreClassloader() {
    if (exploreClassLoader != null) {
      return exploreClassLoader;
    }

    // EXPLORE_CLASSPATH and EXPLORE_CONF_FILES will be defined in startup scripts if Hive is installed.
    String exploreClassPathStr = System.getProperty(Constants.Explore.EXPLORE_CLASSPATH);
    LOG.debug("Explore classpath = {}", exploreClassPathStr);
    if (exploreClassPathStr == null) {
      throw new RuntimeException("System property " + Constants.Explore.EXPLORE_CLASSPATH + " is not set.");
    }

    String exploreConfPathStr = System.getProperty(Constants.Explore.EXPLORE_CONF_FILES);
    LOG.debug("Explore confPath = {}", exploreConfPathStr);
    if (exploreConfPathStr == null) {
      throw new RuntimeException("System property " + Constants.Explore.EXPLORE_CONF_FILES + " is not set.");
    }

    Iterable<File> hiveClassPath = getClassPathJarsFiles(exploreClassPathStr);
    Iterable<File> hiveConfFiles = getClassPathJarsFiles(exploreConfPathStr);
    ImmutableList.Builder<URL> builder = ImmutableList.builder();
    for (File file : Iterables.concat(hiveClassPath, hiveConfFiles)) {
      try {
        if (file.getName().matches(".*\\.xml")) {
          builder.add(file.getParentFile().toURI().toURL());
        } else {
          builder.add(file.toURI().toURL());
        }
      } catch (MalformedURLException e) {
        LOG.error("Jar URL is malformed", e);
        throw Throwables.propagate(e);
      }
    }
    exploreClassLoader = new URLClassLoader(Iterables.toArray(builder.build(), URL.class),
                                            ClassLoader.getSystemClassLoader());
    return exploreClassLoader;
  }

  /**
   * Get all the files contained in a class path.
   */
  public static Iterable<File> getClassPathJarsFiles(String hiveClassPath) {
    if (hiveClassPath == null) {
      return null;
    }
    return Iterables.transform(Splitter.on(':').split(hiveClassPath), STRING_FILE_FUNCTION);
  }

  private static final Function<String, File> STRING_FILE_FUNCTION =
    new Function<String, File>() {
      @Override
      public File apply(String input) {
        return new File(input).getAbsoluteFile();
      }
    };
}
