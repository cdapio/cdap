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

import java.util.Collections;
import java.util.HashMap;
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
    return properties == null ? Collections.unmodifiableMap(new HashMap<String, String>()) : properties;
  }

  public ArtifactSelectorConfig getArtifact() {
    return artifact == null ? new ArtifactSelectorConfig() : artifact;
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
