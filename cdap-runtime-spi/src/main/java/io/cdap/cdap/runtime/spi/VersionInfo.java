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

package io.cdap.cdap.runtime.spi;

/**
 * Version information that is comparable to other instance of the same class or any other Object
 * that returns properly formatted version from it's {@link Object#toString()}. E.g. you can compare
 * it to a string. Also any instance of this interface must return proper bare version from it's
 * toString.
 */
public interface VersionInfo extends Comparable<Object> {

  /**
   * Gets the major version part of the version string. The default implementation is to parse the
   * version string, in the format "major.minor.fix-buildTime"/"major.minor.fix-SNAPSHOT-buildTime".
   *
   * @return the major version.
   */
  default int getMajor() {
    String[] versionInfo = this.toString().split("-");
    if (versionInfo.length > 0) {
      int idx = versionInfo[0].indexOf('.');
      if (idx > 0) {
        return Integer.parseInt(versionInfo[0].substring(0, idx));
      }
    }
    return 0;
  }

  /**
   * Gets the minor version part of the version string. The default implementation is to parse the
   * version string, in the format "major.minor.fix-buildTime"/"major.minor.fix-SNAPSHOT-buildTime".
   *
   * @return the minor version.
   */
  default int getMinor() {
    String[] versionInfo = this.toString().split("-");
    if (versionInfo.length > 0) {
      int major = getMajor();
      int idx = versionInfo[0].indexOf(String.valueOf(major)) + 2;
      int endIdx = versionInfo[0].indexOf('.', idx);
      if (endIdx > 0 && endIdx - idx > 0) {
        return Integer.parseInt(versionInfo[0].substring(idx, endIdx));
      }
    }
    return 0;
  }

  /**
   * Gets the fix version part of the version string. The default implementation is to parse the
   * version string, in the format "major.minor.fix-buildTime"/"major.minor.fix-SNAPSHOT-buildTime".
   *
   * @return the fix version.
   */
  default int getFix() {
    String[] versionInfo = this.toString().split("-");
    if (versionInfo.length > 0) {
      int major = getMajor();
      int idx = versionInfo[0].indexOf(String.valueOf(major)) + 2;
      int midIdx = versionInfo[0].indexOf('.', idx) + 1;
      if (midIdx > 0 && midIdx - idx > 0) {
        int endIdx = versionInfo[0].indexOf('-', midIdx);
        if (endIdx > 0 && endIdx - idx > 0) {
          return Integer.parseInt(versionInfo[0].substring(midIdx, endIdx));
        }
      }
    }
    return 0;
  }

  /**
   * checks if the version is snapshot. The default implementation is to parse the version string,
   * in the format "major.minor.fix-buildTime"/"major.minor.fix-SNAPSHOT-buildTime".
   *
   * @return true if version is snapshot, otherwise false.
   */
  default boolean isSnapshot() {
    String[] versionInfo = this.toString().split("-");
    if (versionInfo.length > 1) {
      return "SNAPSHOT".equals(versionInfo[1]);
    }
    return false;
  }

  /**
   * Gets the buildTime part of the version string. The default implementation is to parse the
   * version string, in the format "major.minor.fix-buildTime"/"major.minor.fix-SNAPSHOT-buildTime".
   *
   * @return the buildTime.
   */
  default long getBuildTime() {
    String[] versionInfo = this.toString().split("-");
    if (isSnapshot() && versionInfo.length > 2) {
      return Long.parseLong(versionInfo[2]);
    }
    if (!isSnapshot() && versionInfo.length > 1) {
      return Long.parseLong(versionInfo[1]);
    }
    return 0;
  }

}
