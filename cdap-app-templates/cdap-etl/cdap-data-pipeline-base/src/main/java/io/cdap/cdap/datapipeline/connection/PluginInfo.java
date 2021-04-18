/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.datapipeline.connection;

import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;

import java.util.Map;
import java.util.Objects;

/**
 * Plugin information inside a connection
 */
public class PluginInfo {
  private final String name;
  private final String type;
  private final String category;
  private final Map<String, String> properties;
  private final ArtifactSelectorConfig artifact;

  public PluginInfo(String name, String type, String category, Map<String, String> properties,
                    ArtifactSelectorConfig artifact) {
    this.name = name;
    this.type = type;
    this.category = category;
    this.properties = properties;
    this.artifact = artifact;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public String getCategory() {
    return category;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public ArtifactSelectorConfig getArtifact() {
    return artifact;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PluginInfo that = (PluginInfo) o;
    return Objects.equals(name, that.name) &&
      Objects.equals(type, that.type) &&
      Objects.equals(category, that.category) &&
      Objects.equals(properties, that.properties) &&
      Objects.equals(artifact, that.artifact);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, category, properties, artifact);
  }
}
