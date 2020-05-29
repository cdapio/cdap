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

package io.cdap.cdap.api.app;

import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.artifact.ArtifactId;
import java.util.HashMap;
import java.util.Map;

/**
 * Stores results of upgrading an application config like new config, artifact upgrade map etc.
 *
 * @param <T> {@link Config} config class that represents the configuration type of an Application.
 */
public class ConfigUpgradeResult<T extends Config> {

  // Upgraded config.
  private T newConfig;
  // Stores mapping of old and new artifact after an upgrade in form <oldArtifact, newArtifact>.
  private Map<ArtifactId, ArtifactId> upgradedArtifacts;

  private ConfigUpgradeResult(T newConfig, Map<ArtifactId, ArtifactId> upgradedArtifacts) {

    this.newConfig = newConfig;
    this.upgradedArtifacts = upgradedArtifacts;
  }

  public T getNewConfig() {
    return newConfig;
  }

  public Map<ArtifactId, ArtifactId> getUpgradedArtifacts() {
    return upgradedArtifacts;
  }


  /**
   * Builder for creating config upgrade result.
   *
   * @param <T> {@link Config} config class that represents the configuration type of an
   * Application.
   */
  public static class Builder<T extends Config> {

    protected T newConfig;
    protected Map<ArtifactId, ArtifactId> upgradedArtifacts;

    protected Builder(T newConfig) {
      this.newConfig = newConfig;
      this.upgradedArtifacts = new HashMap<>();
    }

    public Builder addUpgradeArtifact(ArtifactId oldArtifact, ArtifactId newArtifact) {
      this.upgradedArtifacts.put(oldArtifact, newArtifact);
      return this;
    }

    public Builder setUpgradeArtifacts(Map<ArtifactId, ArtifactId> upgradedArtifacts) {
      this.upgradedArtifacts = new HashMap<>(upgradedArtifacts);
      return this;
    }

    public ConfigUpgradeResult build() {
      return new ConfigUpgradeResult(newConfig, upgradedArtifacts);
    }
  }
}

