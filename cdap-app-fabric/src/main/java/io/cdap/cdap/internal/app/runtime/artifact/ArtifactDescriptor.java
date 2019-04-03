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

import co.cask.cdap.api.artifact.ArtifactId;
import org.apache.twill.filesystem.Location;

/**
 * Uniquely describes an artifact. Artifact descriptors are ordered by scope,
 * then by name, and finally by version.
 */
public final class ArtifactDescriptor implements Comparable<ArtifactDescriptor> {
  private final ArtifactId artifactId;
  private final Location location;

  public ArtifactDescriptor(ArtifactId artifactId, Location location) {
    this.artifactId = artifactId;
    this.location = location;
  }

  /**
   * get artifact Id
   * @return {@link ArtifactId}
   */
  public ArtifactId getArtifactId() {
    return artifactId;
  }

  /**
   * get location of artifact
   * @return {@link Location} of artifact
   */
  public Location getLocation() {
    return location;
  }

  @Override
  public String toString() {
    return "ArtifactDescriptor{" +
      "artifactId=" + artifactId +
      ", location=" + location +
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

    ArtifactDescriptor that = (ArtifactDescriptor) o;
    return compareTo(that) == 0;
  }

  @Override
  public int hashCode() {
    return artifactId.hashCode();
  }

  @Override
  public int compareTo(ArtifactDescriptor other) {
    return getArtifactId().compareTo(other.getArtifactId());
  }
}
