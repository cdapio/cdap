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

package co.cask.cdap.proto.artifact;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.plugin.PluginPropertyField;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents an plugin info returned by
 * /artifacts/{artifact-name}/versions/{artifact-version}/extensions/{plugin-type}/plugins/{plugin-name}
 */
@Beta
public class PluginInfo extends PluginSummary {
  private final Map<String, PluginPropertyField> properties;
  private final Set<String> endpoints;

  public PluginInfo(String name, String type, String description, String className,
                    ArtifactSummary artifact, Map<String, PluginPropertyField> properties, Set<String> endpoints) {
    super(name, type, description, className, artifact);
    this.properties = properties;
    this.endpoints = endpoints;
  }

  public Map<String, PluginPropertyField> getProperties() {
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

    PluginInfo that = (PluginInfo) o;

    return super.equals(that) &&
      Objects.equals(properties, that.properties) &&
      Objects.equals(endpoints, that.endpoints);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), properties);
  }

  @Override
  public String toString() {
    return "PluginSummary{" +
      "name='" + name + '\'' +
      ", type='" + type + '\'' +
      ", description='" + description + '\'' +
      ", className='" + className + '\'' +
      ", properties=" + properties +
      ", artifact=" + artifact +
      ", endpoints=" + endpoints +
      '}';
  }
}
