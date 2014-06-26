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
  /**
   * Hive support enum.
   */
  public enum HiveSuport {
    // todo populate this with whatever hive version CDH4.3 runs with - REACTOR-229
    HIVE_12(Hive12ExploreService.class),
    HIVE_13(Hive13ExploreService.class);

    private Class hiveExploreServiceClass;

    private HiveSuport(Class hiveExploreServiceClass) {
      this.hiveExploreServiceClass = hiveExploreServiceClass;
    }

    public Class getHiveExploreServiceClass() {
      return hiveExploreServiceClass;
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
    HiveSuport hiveVersion = checkHiveSupport(null);
    Class hiveServiceCl = hiveVersion.getHiveExploreServiceClass();
    return hiveServiceCl;
  }

  /**
   * Check that Hive is in the class path - with a right version.
   *
   * @param hiveClassLoader class loader to use to load hive classes.
   *                        If null, the class loader of this class is used.
   */
  public static HiveSuport checkHiveSupport(ClassLoader hiveClassLoader) {
    try {
      ClassLoader usingCL = hiveClassLoader;
      if (usingCL == null) {
        usingCL = ExploreServiceUtils.class.getClassLoader();
      }

      // In Hive 12, CLIService.getOperationStatus returns OperationState.
      // In Hive 13, CLIService.getOperationStatus returns OperationStatus.
      Class cliServiceClass = usingCL.loadClass("org.apache.hive.service.cli.CLIService");
      Class operationHandleCl = usingCL.loadClass("org.apache.hive.service.cli.OperationHandle");
      Method getOperationMethod = cliServiceClass.getDeclaredMethod("getOperationStatus", operationHandleCl);

      // Rowset is an interface in Hive 13, but a class in Hive 12
      Class rowSetClass = usingCL.loadClass("org.apache.hive.service.cli.RowSet");

      if (rowSetClass.isInterface()
        && getOperationMethod.getReturnType() == usingCL.loadClass("org.apache.hive.service.cli.OperationStatus")) {
        return HiveSuport.HIVE_13;
      } else if (!rowSetClass.isInterface()
        && getOperationMethod.getReturnType() == usingCL.loadClass("org.apache.hive.service.cli.OperationState")) {
        return HiveSuport.HIVE_12;
      }
      throw new RuntimeException("Hive distribution not supported.");
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new RuntimeException("Hive jars not present in classpath", e);
    }
  }
}
