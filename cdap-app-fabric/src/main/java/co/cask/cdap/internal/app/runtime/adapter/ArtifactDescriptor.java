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

package co.cask.cdap.internal.app.runtime.adapter;

import co.cask.cdap.proto.Id;
import org.apache.twill.filesystem.Location;

/**
 * Uniquely describes an artifact. Artifact descriptors are ordered by scope,
 * then by name, and finally by version.
 */
public final class ArtifactDescriptor implements Comparable<ArtifactDescriptor> {
  private final Id.Artifact artifact;
  private final Location location;

  public ArtifactDescriptor(Id.Artifact artifact, Location location) {
    this.artifact = artifact;
    this.location = location;
  }

  public Id.Artifact getArtifact() {
    return artifact;
  }

  public Location getLocation() {
    return location;
  }

  @Override
  public String toString() {
    return "ArtifactDescriptor{" +
      "artifact=" + artifact +
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
    return artifact.hashCode();
  }

  @Override
  public int compareTo(ArtifactDescriptor other) {
    return getArtifact().compareTo(other.getArtifact());
  }
}
