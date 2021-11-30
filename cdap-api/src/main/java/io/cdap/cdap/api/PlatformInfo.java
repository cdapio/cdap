/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Objects;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * Accessor class for providing project information.
 */
public final class PlatformInfo {

  private static final Logger LOG = LoggerFactory.getLogger(PlatformInfo.class);
  private static final Version VERSION;

  // Initialize VERSION from build.properties file.
  static {
    Version version = new Version(null);
    try {
      Properties buildProp = new Properties();
      try (InputStream input = PlatformInfo.class.getResourceAsStream("/build.properties")) {
        buildProp.load(input);

        String versionStr = buildProp.getProperty("project.info.version");
        String buildTimeStr = buildProp.getProperty("project.info.build.time");

        if (versionStr != null && buildTimeStr != null) {
          long buildTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").parse(buildTimeStr).getTime();
          version = new Version(String.format("%s-%d", versionStr, buildTime));
        }
      }
    } catch (Exception e) {
      LOG.warn("No BuildInfo available: {}", e.getMessage(), e);
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
  public static final class Version implements VersionInfo {
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
                if (suffix.length() > ("SNAPSHOT".length() + 1)) {
                  suffix = suffix.substring("SNAPSHOT".length() + 1);
                } else {
                  suffix = "";
                }
              }
              if (!suffix.equals("")) {
                buildTime = Long.parseLong(suffix);
              }

            } else {
              //Release version style = Major.Min.Fix
              //Build time set to max to ensure it's after any other
              fix = Integer.parseInt(version.substring(idx));
              buildTime = Long.MAX_VALUE;
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
      } else if (buildTime == Long.MAX_VALUE) {
        return String.format("%d.%d.%d", major, minor, fix);
      }
      return String.format("%d.%d.%d-%d", major, minor, fix, buildTime);
    }

    @Override
    public int compareTo(Object other) {
      Version o;
      if (other instanceof Version) {
        o = (Version) other;
      } else {
        o = new Version(other.toString());
      }
      // Version comparison by major.minor.fix
      int cmp = Integer.compare(major, o.major);
      if (cmp != 0) {
        return cmp;
      }
      cmp = Integer.compare(minor, o.minor);
      if (cmp != 0) {
        return cmp;
      }
      cmp = Integer.compare(fix, o.fix);
      if (cmp != 0) {
        return cmp;
      }
      // Non-snapshot is greater than snapshot
      if (snapshot != o.snapshot) {
        return snapshot ? -1 : 1;
      }

      // If versions are the same, the earlier build is smaller.
      return Long.compare(buildTime, o.buildTime);
    }

    @Override
    public boolean equals(Object obj) {
      return obj != null && compareTo(obj) == 0;
    }

    @Override
    public int hashCode() {
      return Objects.hash(major, minor, fix, snapshot, buildTime);
    }
  }

  private PlatformInfo() {
  }
}

