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

import co.cask.cdap.api.plugin.PluginSelector;
import co.cask.cdap.etl.proto.ArtifactSelector;
import co.cask.cdap.etl.proto.ArtifactSelectorConfig;

import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Plugin Configuration that is part of {@link ETLStage}.
 */
public class Plugin {
  private final String name;
  private final Map<String, String> properties;
  private final ArtifactSelectorConfig artifact;

  public Plugin(String name, Map<String, String> properties, @Nullable ArtifactSelectorConfig artifact) {
    this.name = name;
    this.properties = properties;
    this.artifact = artifact;
  }

  public Plugin(String name, Map<String, String> properties) {
    this(name, properties, null);
  }

  public String getName() {
    return name;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * @param pluginType the plugin type to get a selector for. Only used for error messages when no matching artifact
   *                   for the plugin is found
   * @param pluginName the plugin name to get a selector for. Only used for error messages when no matching artifact
   *                   for the plugin is found
   * @return the plugin selector for this plugin. If artifact settings have been given, the selector will try to
   *         match the specified artifact settings using an {@link ArtifactSelector}.
   *         If not, the default {@link PluginSelector} is returned.
   */
  public PluginSelector getPluginSelector(String pluginType, String pluginName) {
    return artifact == null ? new PluginSelector() : artifact.getArtifactSelector(pluginType, pluginName);
  }

  @Override
  public String toString() {
    return "Plugin{" +
      "name='" + name + '\'' +
      ", properties=" + properties +
      ", artifact=" + artifact +
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

    Plugin that = (Plugin) o;

    return Objects.equals(name, that.name) &&
      Objects.equals(properties, that.properties) &&
      Objects.equals(artifact, that.artifact);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, properties, artifact);
  }
}
