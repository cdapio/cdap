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
import co.cask.cdap.api.artifact.ArtifactDescriptor;
import co.cask.cdap.proto.Id;

import java.util.Objects;

/**
 * Represents an artifact returned by /artifacts and /artifacts/{artifact-name}.
 */
@Beta
public class ArtifactSummary {
  protected final String name;
  protected final String version;
  protected final boolean isSystem;

  public static ArtifactSummary from(Id.Artifact artifactId) {
    return new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion(),
                               Id.Namespace.SYSTEM.equals(artifactId.getNamespace()));
  }

  public static ArtifactSummary from(ArtifactDescriptor descriptor) {
    return new ArtifactSummary(descriptor.getName(), descriptor.getVersion().getVersion(), descriptor.isSystem());
  }

  public ArtifactSummary(String name, String version, boolean isSystem) {
    this.name = name;
    this.version = version;
    this.isSystem = isSystem;
  }

  public String getName() {
    return name;
  }

  public String getVersion() {
    return version;
  }

  public boolean isSystem() {
    return isSystem;
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

    return Objects.equals(name, that.name) && Objects.equals(version, that.version) && isSystem == that.isSystem;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, version, isSystem);
  }

  @Override
  public String toString() {
    return "ArtifactSummary{" +
      "name='" + name + '\'' +
      ", version='" + version + '\'' +
      ", isSystem=" + isSystem +
      '}';
  }
}
