package com.continuuity.hive.server;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;

import java.lang.reflect.Method;

/**
 * Hive Server 2 service.
 */
public abstract class HiveServer extends AbstractIdleService {

  // todo populate this with whatever hive version CDH4.3 runs with
  private static final String[] SUPPORTED_VERSIONS = new String[] { "0.12", "0.13" };

  /**
   * Check that Hive is in the class path - with a right version.
   */
  public static void checkHiveVersion() {
    try {
      Class<?> hiveVersionClass = Class.forName("org.apache.hive.common.util.HiveVersionInfo");
      Method m = hiveVersionClass.getMethod("getVersion");
      String version = (String) m.invoke(null);
      for (int i = 0; i < SUPPORTED_VERSIONS.length; i++) {
        if (version.startsWith(SUPPORTED_VERSIONS[i])) {
          return;
        }
      }
      throw new RuntimeException("Hive version " + version + " is not supported.");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Hive jars not present in classpath");
    } catch (Throwable e) {
      Throwables.propagate(e);
    }
  }
}
