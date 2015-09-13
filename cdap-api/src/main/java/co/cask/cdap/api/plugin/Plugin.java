/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.api.plugin;

import co.cask.cdap.api.artifact.ArtifactId;

import java.util.Objects;

/**
 * A container class for holding plugin information.
 */
public final class Plugin {
  private final ArtifactId artifactId;
  private final PluginClass pluginClass;
  private final PluginProperties properties;

  public Plugin(ArtifactId artifactId, PluginClass pluginClass, PluginProperties properties) {
    this.artifactId = artifactId;
    this.pluginClass = pluginClass;
    this.properties = properties;
  }

  /**
   * @return artifact id
   */
  public ArtifactId getArtifactId() {
    return artifactId;
  }

  /**
   * @return {@link PluginClass}
   */
  public PluginClass getPluginClass() {
    return pluginClass;
  }

  /**
   * Returns the set of properties available when the plugin was created.
   */
  public PluginProperties getProperties() {
    return properties;
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
    return Objects.equals(artifactId, that.artifactId)
      && Objects.equals(pluginClass, that.pluginClass)
      && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(artifactId, pluginClass, properties);
  }

  @Override
  public String toString() {
    return "Plugin{" +
      "artifactId=" + artifactId +
      ",pluginClass=" + pluginClass +
      ",properties=" + properties +
      '}';
  }
}
