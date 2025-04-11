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

import java.util.Objects;

class PluginKey {

  private final String parentName;
  private final String parentNamespace;
  private final String artifactName;
  private final String artifactNamespace;
  private final String artifactVersion;
  private final String pluginType;
  private final String pluginName;

  public PluginKey(String parentName, String parentNamespace, String artifactName,
      String artifactNamespace, String artifactVersion, String pluginType, String pluginName) {
    this.parentName = parentName;
    this.parentNamespace = parentNamespace;
    this.artifactName = artifactName;
    this.artifactNamespace = artifactNamespace;
    this.artifactVersion = artifactVersion;
    this.pluginType = pluginType;
    this.pluginName = pluginName;
  }

  public String getParentName() {
    return parentName;
  }

  public String getParentNamespace() {
    return parentNamespace;
  }

  public String getArtifactName() {
    return artifactName;
  }

  public String getArtifactNamespace() {
    return artifactNamespace;
  }

  public String getArtifactVersion() {
    return artifactVersion;
  }

  public String getPluginType() {
    return pluginType;
  }

  public String getPluginName() {
    return pluginName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PluginKey)) {
      return false;
    }
    PluginKey pluginKey = (PluginKey) o;
    return Objects.equals(parentName, pluginKey.parentName) && Objects.equals(
        parentNamespace, pluginKey.parentNamespace) && Objects.equals(artifactName,
        pluginKey.artifactName) && Objects.equals(artifactNamespace,
        pluginKey.artifactNamespace) && Objects.equals(artifactVersion,
        pluginKey.artifactVersion) && Objects.equals(pluginType, pluginKey.pluginType)
        && Objects.equals(pluginName, pluginKey.pluginName);
  }

  @Override
  public String toString() {
    return "PluginKey{" +
        "parentName='" + parentName + '\'' +
        ", parentNamespace='" + parentNamespace + '\'' +
        ", artifactName='" + artifactName + '\'' +
        ", artifactNamespace='" + artifactNamespace + '\'' +
        ", artifactVersion='" + artifactVersion + '\'' +
        ", pluginType='" + pluginType + '\'' +
        ", pluginName='" + pluginName + '\'' +
        '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(parentName, parentNamespace, artifactName, artifactNamespace,
        artifactVersion,
        pluginType, pluginName);
  }
}