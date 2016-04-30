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

package co.cask.cdap.etl.spec;

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;

/**
 * Specification for a plugin.
 *
 * This is like an {@link ETLPlugin}, but has additional attributes calculated at configure time of the application.
 * The spec contains the artifact selected for the plugin.
 */
public class PluginSpec {
  private final String type;
  private final String name;
  private final Map<String, String> properties;
  private final ArtifactId artifact;

  public PluginSpec(String type, String name, Map<String, String> properties, ArtifactId artifact) {
    this.type = type;
    this.name = name;
    this.properties = ImmutableMap.copyOf(properties);
    this.artifact = artifact;
  }

  public String getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public ArtifactId getArtifact() {
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

    PluginSpec that = (PluginSpec) o;

    return Objects.equals(type, that.type) &&
      Objects.equals(name, that.name) &&
      Objects.equals(properties, that.properties) &&
      Objects.equals(artifact, that.artifact);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, name, properties, artifact);
  }

  @Override
  public String toString() {
    return "PluginSpec{" +
      "type='" + type + '\'' +
      ", name='" + name + '\'' +
      ", properties=" + properties +
      ", artifact=" + artifact +
      '}';
  }
}
