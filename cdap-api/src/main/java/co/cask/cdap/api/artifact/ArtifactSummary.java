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

import java.util.Objects;

/**
 * Represents an artifact returned by /artifacts and /artifacts/{artifact-name}.
 */
@Beta
public class ArtifactSummary {
  protected final String name;
  protected final String version;
  protected final ArtifactScope scope;

  public static ArtifactSummary from(ArtifactId artifactId) {
    return new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion(), artifactId.getScope());
  }

  public ArtifactSummary(String name, String version) {
    this(name, version, ArtifactScope.USER);
  }

  public ArtifactSummary(String name, String version, ArtifactScope scope) {
    this.name = name;
    this.version = version;
    this.scope = scope;
  }

  /**
   * get the name of the artifact
   * @return name of the artifact
   */
  public String getName() {
    return name;
  }

  /**
   * get the version of artifact
   * @return artifact version
   */
  public String getVersion() {
    return version;
  }

  /**
   * get the scope of the artifact
   * @return artifact scope
   */
  public ArtifactScope getScope() {
    return scope;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArtifactSummary that = (ArtifactSummary) o;

    return Objects.equals(name, that.name) &&
      Objects.equals(version, that.version) &&
      Objects.equals(scope, that.scope);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, version, scope);
  }

  @Override
  public String toString() {
    return String.valueOf(scope) + ":" + name + "-" + version;
  }
}
