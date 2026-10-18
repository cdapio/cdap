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
 * Upgrade details of a plugin. This extends the Artifact Upgrade detail class.
 */
public class PluginUpgradeDetail extends ArtifactUpgradeDetail {

  private final String pluginName;
  private final String pluginType;

  public PluginUpgradeDetail(ArtifactUpgradeDetail artifactUpgradeDetail,
      String pluginName, String pluginType) {
    super(artifactUpgradeDetail.getArtifactName(), artifactUpgradeDetail.getCurrentVersion(),
        artifactUpgradeDetail.getLatestVersion());
    this.pluginName = pluginName;
    this.pluginType = pluginType;
  }

  public String getPluginName() {
    return pluginName;
  }

  public String getPluginType() {
    return pluginType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PluginUpgradeDetail)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    PluginUpgradeDetail that = (PluginUpgradeDetail) o;
    return Objects.equals(pluginName, that.pluginName) && Objects.equals(
        pluginType, that.pluginType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), pluginName, pluginType);
  }
}
