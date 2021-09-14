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

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents an plugin info returned by
 * /artifacts/{artifact-name}/versions/{artifact-version}/extensions/{plugin-type}
 * TODO: Check if backwards compatibility is needed here
 */
@Beta
public class PluginSummary {
  protected final String name;
  protected final String type;
  protected final String category;
  protected final String description;
  @Deprecated
  protected final String className;
  protected final List<String> runtimeClassNames;
  protected final ArtifactSummary artifact;

  public PluginSummary(String name, String type, @Nullable String category, String className,
                       List<String> runtimeClassNames, ArtifactSummary artifact, String description) {
    this.name = name;
    this.type = type;
    this.category = category;
    this.runtimeClassNames = runtimeClassNames;
    this.description = description;
    this.className = className;
    this.artifact = artifact;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  @Nullable
  public String getCategory() {
    return category;
  }

  public String getDescription() {
    return description;
  }

  @Deprecated
  public String getClassName() {
    return className;
  }

  public List<String> getRuntimeClassNames() {
    return runtimeClassNames;
  }

  public ArtifactSummary getArtifact() {
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

    PluginSummary that = (PluginSummary) o;

    return Objects.equals(name, that.name) &&
      Objects.equals(type, that.type) &&
      Objects.equals(category, that.category) &&
      Objects.equals(description, that.description) &&
      Objects.equals(className, that.className) &&
      Objects.equals(runtimeClassNames, that.runtimeClassNames) &&
      Objects.equals(artifact, that.artifact);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, category, description, className, runtimeClassNames, artifact);
  }

  @Override
  public String toString() {
    return "PluginSummary{" +
      "name='" + name + '\'' +
      ", type='" + type + '\'' +
      ", category='" + category + '\'' +
      ", description='" + description + '\'' +
      ", className='" + className + '\'' +
      ", runtimeClassNames=" + runtimeClassNames +
      ", artifact=" + artifact +
      '}';
  }
}
