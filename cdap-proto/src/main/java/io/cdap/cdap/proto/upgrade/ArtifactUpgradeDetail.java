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

  private final String artifactName;
  private final String currentVersion;
  private final String latestVersion;
  private final boolean upgradable;
  
  public ArtifactUpgradeDetail(String artifactName, String currentVersion,
      String latestVersion) {
    this.artifactName = artifactName;
    this.currentVersion = currentVersion;
    this.latestVersion = latestVersion;
    this.upgradable = !latestVersion.equalsIgnoreCase(currentVersion);
  }

  public String getArtifactName() {
    return artifactName;
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
    return upgradable == that.upgradable && Objects.equals(artifactName, that.artifactName)
        && Objects.equals(currentVersion, that.currentVersion) && Objects.equals(
        latestVersion, that.latestVersion);
  }

  @Override
  public int hashCode() {
    return Objects.hash(artifactName, currentVersion, latestVersion, upgradable);
  }
}
