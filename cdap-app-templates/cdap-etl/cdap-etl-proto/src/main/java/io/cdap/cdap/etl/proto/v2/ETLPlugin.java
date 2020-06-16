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

package io.cdap.cdap.etl.proto.v2;

import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Plugin Configuration that is part of {@link ETLStage}.
 */
public class ETLPlugin {
  private final String name;
  private final String type;
  private final Map<String, String> properties;
  private final ArtifactSelectorConfig artifact;

  // Only for serialization/deserialization purpose for config upgrade to not lose data set by UI during update.
  private final String label;

  public ETLPlugin(String name, String type, Map<String, String> properties) {
    this(name, type, properties, null, null);
  }

  public ETLPlugin(String name,
                   String type,
                   Map<String, String> properties,
                   @Nullable ArtifactSelectorConfig artifact) {
    this(name, type, properties, artifact, null);
  }

  public ETLPlugin(String name,
                   String type,
                   Map<String, String> properties,
                   @Nullable ArtifactSelectorConfig artifact,
                   String label) {
    this.name = name;
    this.type = type;
    this.properties = Collections.unmodifiableMap(properties);
    this.artifact = artifact;
    this.label = label;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public String getLabel() {
    return label;
  }

  public Map<String, String> getProperties() {
    return Collections.unmodifiableMap(properties == null ? new HashMap<>() : properties);
  }

  public PluginProperties getPluginProperties() {
    if (properties == null || properties.isEmpty()) {
      return PluginProperties.builder().build();
    }
    return PluginProperties.builder().addAll(properties).build();
  }

  @Nullable
  public ArtifactSelectorConfig getArtifactConfig() {
    return artifact;
  }

  /**
   * Validate correctness. Since this object is created through deserialization, some fields that should not be null
   * may be null.
   *
   * @throws IllegalArgumentException if the object is invalid
   */
  public void validate() {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("Invalid plugin " + toString() + ": name must be specified.");
    }
    if (type == null || type.isEmpty()) {
      throw new IllegalArgumentException("Invalid plugin " + toString() + ": type must be specified.");
    }
  }

  @Override
  public String toString() {
    return "Plugin{" +
      "name='" + name + '\'' +
      ", type='" + type + '\'' +
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

    ETLPlugin that = (ETLPlugin) o;

    return Objects.equals(name, that.name) &&
      Objects.equals(type, that.type) &&
      Objects.equals(properties, that.properties) &&
      Objects.equals(artifact, that.artifact);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, properties, artifact);
  }
}
