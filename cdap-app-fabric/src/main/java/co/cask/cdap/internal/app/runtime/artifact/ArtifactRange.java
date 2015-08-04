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

package co.cask.cdap.internal.app.runtime.artifact;

import co.cask.cdap.internal.artifact.ArtifactVersion;
import co.cask.cdap.proto.Id;

import java.util.Objects;

/**
 * Represents a range of versions for an artifact. The lower version is inclusive and the upper version is exclusive.
 */
public class ArtifactRange {
  private final Id.Namespace namespace;
  private final String name;
  private final ArtifactVersion lower;
  private final ArtifactVersion upper;

  public ArtifactRange(Id.Namespace namespace, String name, ArtifactVersion lower, ArtifactVersion upper) {
    this.namespace = namespace;
    this.name = name;
    this.lower = lower;
    this.upper = upper;
  }

  public Id.Namespace getNamespace() {
    return namespace;
  }

  public String getName() {
    return name;
  }

  public ArtifactVersion getLower() {
    return lower;
  }

  public ArtifactVersion getUpper() {
    return upper;
  }

  public boolean versionIsInRange(ArtifactVersion version) {
    return version.compareTo(lower) >= 0 && version.compareTo(upper) < 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArtifactRange that = (ArtifactRange) o;

    return Objects.equals(namespace, that.namespace) &&
      Objects.equals(name, that.name) &&
      Objects.equals(lower, that.lower) &&
      Objects.equals(upper, that.upper);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, name, lower, upper);
  }

  @Override
  public String toString() {
    return "ArtifactRange{" +
      "namespace=" + namespace +
      ", name='" + name + '\'' +
      ", lower=" + lower +
      ", upper=" + upper +
      '}';
  }
}
