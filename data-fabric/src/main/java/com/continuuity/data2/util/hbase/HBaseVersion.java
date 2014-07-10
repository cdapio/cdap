package com.continuuity.data2.util.hbase;

import org.apache.hadoop.hbase.util.VersionInfo;

/**
 * Detects the currently loaded HBase version.  It is assumed that only one HBase version is loaded at a time,
 * since using more than one HBase version within the same process will require classloader isolation anyway.
 */
public class HBaseVersion {
  private static final String HBASE_94_VERSION = "0.94";
  private static final String HBASE_96_VERSION = "0.96";
  private static final String HBASE_98_VERSION = "0.98";

  /**
   * Represents the major version of the HBase library that is currently loaded.
   */
  public enum Version {
    HBASE_94("0.94"),
    HBASE_96("0.96"),
    HBASE_98("0.98"),
    UNKNOWN("unknown");

    final String majorVersion;

    Version(String majorVersion) {
      this.majorVersion = majorVersion;
    }

    public String getMajorVersion() {
      return majorVersion;
    }
  }

  private static Version currentVersion;
  private static String versionString;
  static {
    try {
      versionString = VersionInfo.getVersion();
      if (versionString.startsWith(HBASE_94_VERSION)) {
        currentVersion = Version.HBASE_94;
      } else if (versionString.startsWith(HBASE_96_VERSION)) {
        currentVersion = Version.HBASE_96;
      } else if (versionString.startsWith(HBASE_98_VERSION)) {
        currentVersion = Version.HBASE_98;
      } else {
        currentVersion = Version.UNKNOWN;
      }
    } catch (Throwable e) {
      // must be a class loading exception, HBasde is not there
      currentVersion = Version.UNKNOWN;
    }
  }

  /**
   * Returns the major version of the currently loaded HBase library.
   */
  public static Version get() {
    return currentVersion;
  }

  /**
   * Returns the full version string for the currently loaded HBase library.
   */
  public static String getVersionString() {
    return versionString;
  }

  /**
   * Prints out the HBase {@link Version} enum value for the current version of HBase on the classpath.
   */
  public static void main(String[] args) {
    Version version = HBaseVersion.get();
    System.out.println(version.getMajorVersion());
  }
}
