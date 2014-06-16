package com.continuuity.explore.service;

import com.continuuity.common.conf.StringUtils;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Utility class for the explore service.
 */
public class ExploreServiceUtils {
  // todo populate this with whatever hive version CDH4.3 runs with
  private static final String[] SUPPORTED_VERSIONS = new String[] { "0.12", "0.13" };

  private static Iterable<URL> getClassPath(String hiveClassPath) {
    if (hiveClassPath == null) {
      return null;
    }
    return Iterables.transform(Splitter.on(':').split(hiveClassPath), STRING_URL_FUNCTION);
  }

  private static final Function<String, URL> STRING_URL_FUNCTION =
      new Function<String, URL>() {
        @Override
        public URL apply(String input) {
          try {
            return new File(input).toURI().toURL();
          } catch (MalformedURLException e) {
            throw Throwables.propagate(e);
          }
        }
      };

  /**
   * Builds a class loader with the class path provided.
   */
  public static ClassLoader buildHiveClassLoader(String hiveClassPathStr) {
    Iterable<URL> hiveClassPath = getClassPath(hiveClassPathStr);
    return new URLClassLoader(Iterables.toArray(hiveClassPath, URL.class),
                              ClassLoader.getSystemClassLoader());
  }

  /**
   * Check that Hive is in the class path - with a right version.
   */
  public static void checkHiveVersion(ClassLoader hiveClassLoader) {
    try {
      Class hiveVersionClass = hiveClassLoader.loadClass("org.apache.hive.common.util.HiveVersionInfo");
      Method m = hiveVersionClass.getDeclaredMethod("getVersion");
      String version = (String) m.invoke(null);
      for (String supportedVersion : SUPPORTED_VERSIONS) {
        if (version.startsWith(supportedVersion)) {
          return;
        }
      }
      throw new RuntimeException("Hive version " + version + " is not supported. " +
          "Versions supported begin with one of the following: " +
          StringUtils.arrayToString(SUPPORTED_VERSIONS));
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new RuntimeException("Hive jars not present in classpath", e);
    }
  }
}
