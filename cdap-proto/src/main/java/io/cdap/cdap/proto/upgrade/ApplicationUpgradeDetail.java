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

import java.util.List;
import java.util.Objects;

/**
 * Upgrade details for an application consisting of application artifact and plugin upgrade details.
 */
public class ApplicationUpgradeDetail {

  private final String name;
  private final ArtifactUpgradeDetail applicationArtifactUpgradeDetail;
  private final List<PluginUpgradeDetail> pluginUpgradeDetails;
  private final boolean upgradable;


  public ApplicationUpgradeDetail(String name, ArtifactUpgradeDetail applicationArtifactUpgradeDetail,
      List<PluginUpgradeDetail> pluginUpgradeDetails) {
    this.name = name;
    this.applicationArtifactUpgradeDetail = applicationArtifactUpgradeDetail;
    this.pluginUpgradeDetails = pluginUpgradeDetails;
    this.upgradable =
        applicationArtifactUpgradeDetail.isUpgradable() || pluginUpgradeDetails.stream()
            .anyMatch(ArtifactUpgradeDetail::isUpgradable);
  }

  public String getName() {
    return name;
  }

  public ArtifactUpgradeDetail getApplicationArtifactUpgradeDetail() {
    return applicationArtifactUpgradeDetail;
  }

  public List<PluginUpgradeDetail> getPluginUpgradeDetails() {
    return pluginUpgradeDetails;
  }

  public boolean isUpgradable() {
    return upgradable;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ApplicationUpgradeDetail)) {
      return false;
    }
    ApplicationUpgradeDetail that = (ApplicationUpgradeDetail) o;
    return upgradable == that.upgradable && Objects.equals(name, that.name)
        && Objects.equals(applicationArtifactUpgradeDetail,
        that.applicationArtifactUpgradeDetail) && Objects.equals(pluginUpgradeDetails,
        that.pluginUpgradeDetails);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, applicationArtifactUpgradeDetail, pluginUpgradeDetails, upgradable);
  }
}
