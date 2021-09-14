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

package io.cdap.cdap.proto.artifact;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginPropertyField;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents an plugin info returned by
 * /artifacts/{artifact-name}/versions/{artifact-version}/extensions/{plugin-type}/plugins/{plugin-name}
 */
@Beta
public class PluginInfo extends PluginSummary {

  private final String configFieldName;
  private final Map<String, PluginPropertyField> properties;

  public PluginInfo(PluginClass pluginClass, ArtifactSummary artifactSummary) {
    this(pluginClass.getName(), pluginClass.getType(), pluginClass.getCategory(), pluginClass.getClassName(),
         pluginClass.getRuntimeClassNames(), pluginClass.getConfigFieldName(), artifactSummary,
         pluginClass.getProperties(), pluginClass.getDescription());
  }

  public PluginInfo(String name, String type, String category, String className, List<String> runtimeClassNames,
                    @Nullable String configFieldName, ArtifactSummary artifact,
                    Map<String, PluginPropertyField> properties, String description) {
    super(name, type, category, className, runtimeClassNames, artifact, description);
    this.configFieldName = configFieldName;
    this.properties = properties;
  }

  @Nullable
  public String getConfigFieldName() {
    return configFieldName;
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
      Objects.equals(configFieldName, that.configFieldName) &&
      Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), configFieldName, properties);
  }

  @Override
  public String toString() {
    return "PluginInfo{" +
      "configFieldName='" + configFieldName + '\'' +
      ", properties=" + properties +
      ", name='" + name + '\'' +
      ", type='" + type + '\'' +
      ", description='" + description + '\'' +
      ", className='" + className + '\'' +
      ", artifact=" + artifact +
      ", name='" + getName() + '\'' +
      ", type='" + getType() + '\'' +
      ", description='" + getDescription() + '\'' +
      ", className='" + getClassName() + '\'' +
      ", runtimeСlassNames=" + getRuntimeClassNames() +
      ", artifact=" + getArtifact() +
      '}';
  }
}
