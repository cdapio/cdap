package com.continuuity.hive.server;

import com.continuuity.common.conf.StringUtils;

import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.hive.common.util.HiveVersionInfo;

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
      String version = HiveVersionInfo.getVersion();
      for (int i = 0; i < SUPPORTED_VERSIONS.length; i++) {
        if (version.startsWith(SUPPORTED_VERSIONS[i])) {
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
