package com.continuuity.explore.service;

import com.continuuity.common.conf.StringUtils;

import org.apache.hive.common.util.HiveVersionInfo;

/**
 * Utility class for the explore service.
 */
public class ExploreServiceUtils {
  // todo populate this with whatever hive version CDH4.3 runs with
  private static final String[] SUPPORTED_VERSIONS = new String[] { "0.12", "0.13" };

  /**
   * Check that Hive is in the class path - with a right version.
   */
  public static void checkHiveVersion() {
    try {
      String version = HiveVersionInfo.getVersion();
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
