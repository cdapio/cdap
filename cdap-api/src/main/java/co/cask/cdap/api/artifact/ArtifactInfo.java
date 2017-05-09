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

package co.cask.cdap.api.artifact;

import co.cask.cdap.api.annotation.Beta;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents an artifact returned by /artifacts/{artifact-name}/versions/{artifact-version}.
 */
@Beta
public class ArtifactInfo extends ArtifactSummary {
  private final ArtifactClasses classes;
  private final Map<String, String> properties;
  private final Set<ArtifactRange> parents;

  public ArtifactInfo(ArtifactId id, ArtifactClasses classes, Map<String, String> properties) {
    this(id.getName(), id.getVersion().getVersion(), id.getScope(), classes, properties);
  }

  public ArtifactInfo(ArtifactId id, ArtifactClasses classes, Map<String, String> properties,
                      Set<ArtifactRange> parents) {
    this(id.getName(), id.getVersion().getVersion(), id.getScope(), classes, properties, parents);
  }

  public ArtifactInfo(String name, String version, ArtifactScope scope,
                      ArtifactClasses classes, Map<String, String> properties) {
    super(name, version, scope);
    this.classes = classes;
    this.properties = properties;
    this.parents = null;
  }

  public ArtifactInfo(String name, String version, ArtifactScope scope, ArtifactClasses classes,
                      Map<String, String> properties, Set<ArtifactRange> parents) {
    super(name, version, scope);
    this.classes = classes;
    this.properties = properties;
    this.parents = parents;
  }

  public ArtifactClasses getClasses() {
    return classes;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public Set<ArtifactRange> getParents() {
    return parents;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArtifactInfo that = (ArtifactInfo) o;

    return super.equals(that) &&
      Objects.equals(classes, that.classes) &&
      Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), classes, properties);
  }

  @Override
  public String toString() {
    return "ArtifactInfo{" +
      "name='" + name + '\'' +
      ", version='" + version + '\'' +
      ", scope=" + scope +
      ", classes=" + classes +
      ", properties=" + properties +
      '}';
  }
}
