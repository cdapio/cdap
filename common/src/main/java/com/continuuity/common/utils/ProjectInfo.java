/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.utils;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Accessor class for providing project information.
 */
public final class ProjectInfo {

  private static final Logger LOG = LoggerFactory.getLogger(ProjectInfo.class);
  private static final Version VERSION;

  // Initialize VERSION from the generated BuildInfo.
  static {
    Version version = new Version(null);
    try {
      Class<?> clz = Class.forName("com.continuuity.common.BuildInfo");
      Long buildTime = (Long) clz.getField("BUILD_TIME").get(null);
      String versionStr = (String) clz.getField("VERSION").get(null);

      if (buildTime != null && versionStr != null) {
        version = new Version(String.format("%s-%d", versionStr, buildTime));
      } else {
        LOG.warn("No BuildInfo available. Build time: {}, version: {}", buildTime, versionStr);
      }

    } catch (Exception e) {
      LOG.warn("No BuildInfo available: {}", e.getMessage());
    }
    VERSION = version;
  }

  /**
   * @return the project version.
   */
  public static Version getVersion() {
    return VERSION;
  }


  /**
   * This class encapsulates information about project version.
   */
  public static final class Version implements Comparable<Version> {
    private final int major;
    private final int minor;
    private final int fix;
    private final boolean snapshot;
    private final long buildTime;

    public Version(int major, int minor, int fix, boolean snapshot, long buildTime) {
      this.major = major;
      this.minor = minor;
      this.fix = fix;
      this.snapshot = snapshot;
      this.buildTime = buildTime;
    }

    /**
     * Construct a Version instance by parsing the version string, in the format returned by {@link #toString()}.
     * @param version The version string. If the version is {@code null}, all versions would be {@code 0}.
     */
    public Version(@Nullable String version) {
      int major = 0;
      int minor = 0;
      int fix = 0;
      boolean snapshot = false;
      long buildTime = System.currentTimeMillis();

      if (version != null) {
        // Version string is [major].[minor].[fix](-SNAPSHOT)-[buildTime]
        int idx = version.indexOf('.');
        if (idx > 0) {
          major = Integer.parseInt(version.substring(0, idx));

          idx++;
          int endIdx = version.indexOf('.', idx);
          if (endIdx > 0 && endIdx - idx > 0) {
            minor = Integer.parseInt(version.substring(idx, endIdx));

            idx = endIdx + 1;
            endIdx = version.indexOf('-', idx);
            if (endIdx > 0 && endIdx - idx > 0) {
              fix = Integer.parseInt(version.substring(idx, endIdx));

              idx = endIdx + 1;
              String suffix = version.substring(idx);
              snapshot = suffix.startsWith("SNAPSHOT");
              if (snapshot) {
                suffix = suffix.substring("SNAPSHOT".length() + 1);
              }
              buildTime = Long.parseLong(suffix);
            }
          }
        }
      }
      this.major = major;
      this.minor = minor;
      this.fix = fix;
      this.snapshot = snapshot;
      this.buildTime = buildTime;
    }

    public int getMajor() {
      return major;
    }

    public int getMinor() {
      return minor;
    }

    public int getFix() {
      return fix;
    }

    public boolean isSnapshot() {
      return snapshot;
    }

    public long getBuildTime() {
      return buildTime;
    }

    @Override
    public String toString() {
      if (isSnapshot()) {
        return String.format("%d.%d.%d-SNAPSHOT-%d", major, minor, fix, buildTime);
      }
      return String.format("%d.%d.%d-%d", major, minor, fix, buildTime);
    }

    @Override
    public int compareTo(Version o) {
      // Version comparison by major.minor.fix
      int cmp = Ints.compare(major, o.major);
      if (cmp != 0) {
        return cmp;
      }
      cmp = Ints.compare(minor, o.minor);
      if (cmp != 0) {
        return cmp;
      }
      cmp = Ints.compare(fix, o.fix);
      if (cmp != 0) {
        return cmp;
      }
      // Non-snapshot is greater than snapshot
      if (snapshot != o.snapshot) {
        return snapshot ? -1 : 1;
      }

      // If versions are the same, the earlier build is smaller.
      return Longs.compare(buildTime, o.buildTime);
    }
  }

  private ProjectInfo() {
  }
}
