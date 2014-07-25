/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.explore.jdbc;

/**
 * Utility methods and constants to use in Explore JDBC driver.
 */
public class ExploreJDBCUtils {
  public static final String URI_JDBC_PREFIX = "jdbc:";

  /**
   * Takes a version string delmited by '.' and '-' characters
   * and returns a partial version.
   *
   * @param fullVersion
   *          version string.
   * @param position
   *          position of version string to get starting at 0. eg, for a X.x.xxx
   *          string, 0 will return the major version, 1 will return minor
   *          version.
   * @return version part, or -1 if version string was malformed.
   */
  static int getVersionPart(String fullVersion, int position) {
    try {
      String[] tokens = fullVersion.split("[\\.-]");
      if (tokens != null && tokens.length > 1 && position >= 0 && tokens.length > position
        && tokens[position] != null) {
        return Integer.parseInt(tokens[position]);
      }
    } catch (Throwable e) {
      // Do nothing
    }
    return -1;
  }

}
