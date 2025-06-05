/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.proto.upgrade;

import java.util.Objects;

/**
 * Upgrade details for the artifact consisting of artifact name and version details.
 */
public class ArtifactUpgradeDetail {

  private final String name;
  private final String currentVersion;
  private final String latestVersion;
  private final boolean upgradable;


  public ArtifactUpgradeDetail(String name, String currentVersion,
      String upgradeVersion) {
    this.name = name;
    this.currentVersion = currentVersion;
    this.latestVersion = upgradeVersion;
    this.upgradable = !latestVersion.equalsIgnoreCase(currentVersion);
  }

  public String getName() {
    return name;
  }

  public String getCurrentVersion() {
    return currentVersion;
  }

  public String getLatestVersion() {
    return latestVersion;
  }

  public boolean isUpgradable() {
    return upgradable;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ArtifactUpgradeDetail)) {
      return false;
    }
    ArtifactUpgradeDetail that = (ArtifactUpgradeDetail) o;
    return upgradable == that.upgradable && Objects.equals(name, that.name)
        && Objects.equals(currentVersion, that.currentVersion) && Objects.equals(
        latestVersion, that.latestVersion);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, currentVersion, latestVersion, upgradable);
  }
}
