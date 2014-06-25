package com.continuuity.explore.service;

import com.continuuity.explore.service.hive.Hive12ExploreService;
import com.continuuity.explore.service.hive.Hive13ExploreService;
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
  // todo populate this with whatever hive version CDH4.3 runs with - REACTOR-229

  /**
   * Hive supported versions enum.
   */
  public enum HiveSupportedVersions {
    HIVE_12("0.12", Hive12ExploreService.class),
    HIVE_13("0.13", Hive13ExploreService.class);

    private String versionPrefix;
    private Class hiveExploreServiceClass;

    private HiveSupportedVersions(String versionPrefix, Class hiveExploreServiceClass) {
      this.versionPrefix = versionPrefix;
      this.hiveExploreServiceClass = hiveExploreServiceClass;
    }

    public String getVersionPrefix() {
      return versionPrefix;
    }

    public Class getHiveExploreServiceClass() {
      return hiveExploreServiceClass;
    }

    public static String getSupportedVersionsStr() {
      StringBuilder sb = new StringBuilder();
      for (HiveSupportedVersions version : HiveSupportedVersions.values()) {
        sb.append(version.getVersionPrefix());
        sb.append(",");
      }
      return sb.toString();
    }
  }

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

  public static Class getHiveService() {
    HiveSupportedVersions hiveVersion = checkHiveVersion(null);
    return hiveVersion.getHiveExploreServiceClass();
  }

  /**
   * Check that Hive is in the class path - with a right version.
   *
   * @param hiveClassLoader class loader to use to check hive version.
   *                        If null, the class loader of this class is used.
   */
  public static HiveSupportedVersions checkHiveVersion(ClassLoader hiveClassLoader) {
    try {
      ClassLoader usingCL = hiveClassLoader;
      if (usingCL == null) {
        usingCL = ExploreServiceUtils.class.getClassLoader();
      }
      Class hiveVersionClass = usingCL.loadClass("org.apache.hive.common.util.HiveVersionInfo");
      Method m = hiveVersionClass.getDeclaredMethod("getVersion");
      String version = (String) m.invoke(null);
      for (HiveSupportedVersions supportedVersion : HiveSupportedVersions.values()) {
        if (version.startsWith(supportedVersion.getVersionPrefix())) {
          return supportedVersion;
        }
      }
      throw new RuntimeException("Hive version " + version + " is not supported. " +
                                 "Versions supported begin with one of the following: " +
                                 HiveSupportedVersions.getSupportedVersionsStr());
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new RuntimeException("Hive jars not present in classpath", e);
    }
  }
}
