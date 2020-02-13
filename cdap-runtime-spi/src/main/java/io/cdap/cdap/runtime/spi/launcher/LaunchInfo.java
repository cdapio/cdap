/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.launcher;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class LaunchInfo {
  private final String programId;
  private final String clusterName;
  private final List<LauncherFile> launcherFileList;
  private Map<String, String> properties;

  /**
   *
   * @param programId
   * @param clusterName
   * @param files
   * @param properties
   */
  public LaunchInfo(String programId, String clusterName, List<LauncherFile> files, Map<String, String> properties) {
    this.programId = programId;
    this.clusterName = clusterName;
    this.launcherFileList = files;
    this.properties = properties;
  }

  public String getProgramId() {
    return programId;
  }

  public String getClusterName() {
    return clusterName;
  }

  public List<LauncherFile> getLauncherFileList() {
    return launcherFileList;
  }

  public Map<String, String> getProperties() {
    return properties;
  }
}
