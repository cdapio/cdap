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

package io.cdap.cdap.internal.app.store.adapters;

import com.google.common.base.MoreObjects;
import java.util.Objects;

/**
 * Unique key for identifying a plugin. Used for look up and caching.
 */
class PluginKey {

  private final String parentName;
  private final String parentNamespace;
  private final String artifactName;
  private final String artifactNamespace;
  private final String artifactVersion;
  private final String pluginType;
  private final String pluginName;

  /**
   * Creates a {@code PluginKey} with specified plugin identification details.
   */
  PluginKey(String parentName, String parentNamespace, String artifactName,
      String artifactNamespace, String artifactVersion, String pluginType, String pluginName) {
    this.parentName = parentName;
    this.parentNamespace = parentNamespace;
    this.artifactName = artifactName;
    this.artifactNamespace = artifactNamespace;
    this.artifactVersion = artifactVersion;
    this.pluginType = pluginType;
    this.pluginName = pluginName;
  }

  /**
   * Returns the parent name.
   */
  public String getParentName() {
    return parentName;
  }

  /**
   * Returns the parent namespace.
   */
  public String getParentNamespace() {
    return parentNamespace;
  }

  /**
   * Returns the plugin's artifact name.
   */
  public String getArtifactName() {
    return artifactName;
  }

  /**
   * Returns the plugin's artifact namespace.
   */
  public String getArtifactNamespace() {
    return artifactNamespace;
  }

  /**
   * Returns the plugin's artifact version.
   */
  public String getArtifactVersion() {
    return artifactVersion;
  }

  /**
   * Returns the plugin type.
   */
  public String getPluginType() {
    return pluginType;
  }

  /**
   * Returns the plugin name.
   */
  public String getPluginName() {
    return pluginName;
  }

  /**
   * Checks equality with another object.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PluginKey)) {
      return false;
    }
    PluginKey pluginKey = (PluginKey) o;
    return Objects.equals(parentName, pluginKey.parentName)
        && Objects.equals(parentNamespace, pluginKey.parentNamespace)
        && Objects.equals(artifactName, pluginKey.artifactName)
        && Objects.equals(artifactNamespace, pluginKey.artifactNamespace)
        && Objects.equals(artifactVersion, pluginKey.artifactVersion)
        && Objects.equals(pluginType, pluginKey.pluginType)
        && Objects.equals(pluginName, pluginKey.pluginName);
  }

  /**
   * Returns a string representation of this key.
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("parentName", parentName)
        .add("parentNamespace", parentNamespace)
        .add("artifactName", artifactName)
        .add("artifactNamespace", artifactNamespace)
        .add("artifactVersion", artifactVersion)
        .add("pluginType", pluginType)
        .add("pluginName", pluginName)
        .toString();
  }

  /**
   * Computes the hash code for this key.
   */
  @Override
  public int hashCode() {
    return Objects.hash(parentName, parentNamespace, artifactName, artifactNamespace,
        artifactVersion, pluginType, pluginName);
  }
}
