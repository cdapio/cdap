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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A container class for holding plugin information.
 */
public final class Plugin {
  private final List<ArtifactId> parents;
  private final ArtifactId artifactId;
  private final PluginClass pluginClass;
  private final PluginProperties properties;

  @Deprecated
  public Plugin(ArtifactId artifactId, PluginClass pluginClass, PluginProperties properties) {
    this(new ArrayList<ArtifactId>(), artifactId, pluginClass, properties);
  }

  /**
   * Create a Plugin
   *
   * @param parents iterable of parent plugins. The first parent must be the direct parent of this plugin, and each
   *                subsequent item in the iterable must be the direct parent of the item before it
   * @param artifactId the artifact for this plugin
   * @param pluginClass information about the plugin class
   * @param properties properties of the plugin
   */
  public Plugin(Iterable<ArtifactId> parents, ArtifactId artifactId,
                PluginClass pluginClass, PluginProperties properties) {
    List<ArtifactId> parentList = new ArrayList<>();
    for (ArtifactId parent : parents) {
      parentList.add(parent);
    }
    this.parents = Collections.unmodifiableList(parentList);
    this.artifactId = artifactId;
    this.pluginClass = pluginClass;
    this.properties = properties;
  }

  /**
   * @return the artifact parent chain. Every artifact in the list is the parent of the artifact behind it.
   *         This list does not contain the plugin's artifact, which means it will be an empty list if the plugin's
   *         parent is a program and not another plugin.
   */
  public List<ArtifactId> getParents() {
    // null check for backwards compatibility, since this field was added in 4.3.0
    return parents == null ? Collections.<ArtifactId>emptyList() : parents;
  }

  /**
   * @return artifact id of the plugin
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
    return Objects.equals(getParents(), that.getParents())
      && Objects.equals(artifactId, that.artifactId)
      && Objects.equals(pluginClass, that.pluginClass)
      && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getParents(), artifactId, pluginClass, properties);
  }

  @Override
  public String toString() {
    return "Plugin{" +
      "parents=" + getParents() +
      ", artifactId=" + artifactId +
      ", pluginClass=" + pluginClass +
      ", properties=" + properties +
      '}';
  }
}
