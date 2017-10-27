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

package co.cask.cdap.etl.proto.v1;

import co.cask.cdap.etl.proto.ArtifactSelectorConfig;
import co.cask.cdap.etl.proto.UpgradeContext;

import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * ETL Stage Configuration.
 */
public final class ETLStage {
  private final String name;
  private final Plugin plugin;
  // TODO : can remove the following properties and clean up the constructor after UI support.
  private final Map<String, String> properties;
  private final String errorDatasetName;

  public ETLStage(String name, Plugin plugin, @Nullable String errorDatasetName) {
    this.name = name;
    this.plugin = plugin;
    this.properties = plugin.getProperties();
    this.errorDatasetName = errorDatasetName;
  }

  public ETLStage(String name, Plugin plugin) {
    this(name, plugin, null);
  }

  public ETLStage getCompatibleStage(String stageName) {
    if (plugin != null) {
      return this;
    } else {
      return new ETLStage(stageName, new Plugin(name, properties), errorDatasetName);
    }
  }

  public String getName() {
    return name;
  }

  public Plugin getPlugin() {
    return plugin;
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

  public co.cask.cdap.etl.proto.v2.ETLStage upgradeStage(String type, UpgradeContext upgradeContext) {
    ArtifactSelectorConfig artifactSelectorConfig = upgradeContext.getPluginArtifact(type, plugin.getName());
    if (artifactSelectorConfig == null) {
      artifactSelectorConfig = plugin.getArtifact();
    }
    co.cask.cdap.etl.proto.v2.ETLPlugin etlPlugin = new co.cask.cdap.etl.proto.v2.ETLPlugin(
      plugin.getName(), type, plugin.getProperties(), artifactSelectorConfig);

    if (errorDatasetName != null) {
      throw new IllegalStateException(
        String.format("Cannot upgrade stage '%s'. Error datasets have been replaced by error collectors. " +
                        "Please connect stage '%s' to an error collector, then connect the error collector " +
                        "to a sink.", name, name));
    }
    return new co.cask.cdap.etl.proto.v2.ETLStage(name, etlPlugin);
  }
}
