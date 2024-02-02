/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.proto.app;

import java.util.Objects;

/**
 * Contains an app name and version.
 */
public class AppVersion {

  private final String name;
  private final String appVersion;
  private final String schedulesStr;
  private final String preferenceStr;


  /**
   * Default constructor.
   */
  public AppVersion(String name, String appVersion, String schedulesStr, String preferenceStr) {
    this.name = name;
    this.appVersion = appVersion;
    this.schedulesStr = schedulesStr;
    this.preferenceStr = preferenceStr;
  }


  public String getName() {
    return name;
  }

  public String getAppVersion() {
    return appVersion;
  }

  public String getSchedulesStr() {
    return schedulesStr;
  }

  public String getPreferenceStr() {
    return preferenceStr;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AppVersion)) {
      return false;
    }
    AppVersion that = (AppVersion) o;
    return Objects.equals(name, that.name) && Objects.equals(appVersion, that.appVersion)
        && Objects.equals(schedulesStr, that.schedulesStr) && Objects.equals(preferenceStr,
        that.preferenceStr);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, appVersion, schedulesStr, preferenceStr);
  }

  @Override
  public String toString() {
    return String.format("%s-%s", name, appVersion);
  }
}
