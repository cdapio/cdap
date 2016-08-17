/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.proto.v2;

import co.cask.cdap.etl.proto.ArtifactSelectorConfig;
import co.cask.cdap.etl.proto.UpgradeContext;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * ETL Stage Configuration.
 */
public final class ETLStage {
  private final String name;
  private final ETLPlugin plugin;
  private final String errorDatasetName;

  public ETLStage(String name, ETLPlugin plugin, @Nullable String errorDatasetName) {
    this.name = name;
    this.plugin = plugin;
    this.errorDatasetName = errorDatasetName;
  }

  public ETLStage(String name, ETLPlugin plugin) {
    this(name, plugin, null);
  }

  public String getName() {
    return name;
  }

  public ETLPlugin getPlugin() {
    return plugin;
  }

  @Nullable
  public String getErrorDatasetName() {
    return errorDatasetName;
  }

  /**
   * Validate correctness. Since this object is created through deserialization, some fields that should not be null
   * may be null.
   *
   * @throws IllegalArgumentException if the object is invalid
   */
  public void validate() {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("Invalid stage " + toString() + ": name must be specified.");
    }
    if (plugin == null) {
      throw new IllegalArgumentException("Invalid stage " + toString() + ": plugin must be specified.");
    }
    plugin.validate();
  }

  // used by UpgradeTool to upgrade a 3.4.x stage to 3.5.x, which may include an update of the plugin artifact
  public ETLStage upgradeStage(UpgradeContext upgradeContext) {
    ArtifactSelectorConfig artifactSelectorConfig =
      upgradeContext.getPluginArtifact(plugin.getType(), plugin.getName());
    co.cask.cdap.etl.proto.v2.ETLPlugin etlPlugin = new co.cask.cdap.etl.proto.v2.ETLPlugin(
      plugin.getName(), plugin.getType(), plugin.getProperties(), artifactSelectorConfig);
    return new co.cask.cdap.etl.proto.v2.ETLStage(name, etlPlugin, errorDatasetName);
  }

  @Override
  public String toString() {
    return "ETLStage{" +
      "name='" + name + '\'' +
      ", plugin=" + plugin +
      ", errorDatasetName='" + errorDatasetName + '\'' +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ETLStage that = (ETLStage) o;

    return Objects.equals(name, that.name) &&
      Objects.equals(plugin, that.plugin) &&
      Objects.equals(errorDatasetName, that.errorDatasetName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, plugin, errorDatasetName);
  }

}
