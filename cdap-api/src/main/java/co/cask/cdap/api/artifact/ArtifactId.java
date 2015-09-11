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
 * Uniquely describes an artifact.
 */
@Beta
public final class ArtifactId implements Comparable<ArtifactId> {
  private final String name;
  private final ArtifactVersion version;
  private final ArtifactScope scope;

  public ArtifactId(String name, ArtifactVersion version, ArtifactScope scope) {
    this.name = name;
    this.version = version;
    this.scope = scope;
  }

  public String getName() {
    return name;
  }

  public ArtifactVersion getVersion() {
    return version;
  }

  public ArtifactScope getScope() {
    return scope;
  }

  @Override
  public String toString() {
    return "ArtifactId{" +
      "name='" + name + '\'' +
      ", version=" + version +
      ", scope='" + scope + '\'' +
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

    ArtifactId that = (ArtifactId) o;
    return Objects.equals(name, that.name) &&
      Objects.equals(version, that.version) &&
      Objects.equals(scope, that.scope);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, version, scope);
  }

  @Override
  public int compareTo(ArtifactId other) {
    int cmp = getScope().compareTo(other.getScope());
    if (cmp != 0) {
      return cmp;
    }
    cmp = getName().compareTo(other.getName());
    if (cmp != 0) {
      return cmp;
    }
    return getVersion().compareTo(other.getVersion());
  }
}
