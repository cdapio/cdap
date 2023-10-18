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

package io.cdap.cdap.proto.artifact.artifact;

import io.cdap.cdap.api.artifact.ArtifactId;
import java.net.URI;
import java.util.Objects;
import org.apache.twill.filesystem.Location;

/**
 * Uniquely describes an artifact. Artifact descriptors are ordered by scope, then by name, and
 * finally by version.
 */
public final class ArtifactDescriptor implements Comparable<ArtifactDescriptor> {

  private final String namespace;
  private final ArtifactId artifactId;

  /**
   * Mark with transient to not serialize it. Store the location URI in separate field "locationURI"
   * that gets serialized when this object needs to be transferred through the network. The
   * recipient should instantiate a {@link Location} based on the received "locationURI"
   */
  private final transient Location location;
  private final URI locationURI;

  public ArtifactDescriptor(String namespace, ArtifactId artifactId, Location location) {
    this.namespace = namespace;
    this.artifactId = artifactId;
    this.location = location;
    this.locationURI = location.toURI();
  }

  public ArtifactDescriptor(String namespace, ArtifactId artifactId, URI locationURI) {
    this.namespace = namespace;
    this.artifactId = artifactId;
    this.location = null;
    this.locationURI = locationURI;
  }

  public String getNamespace() {
    return namespace;
  }

  /**
   * get artifact Id
   *
   * @return {@link ArtifactId}
   */
  public ArtifactId getArtifactId() {
    return artifactId;
  }

  /**
   * get location of artifact
   *
   * @return {@link Location} of artifact
   * @deprecated This will be removed in CDAP-19150
   */
  public Location getLocation() {
    return location;
  }

  public URI getLocationURI() {
    return locationURI;
  }

  @Override
  public String toString() {
    return "ArtifactDescriptor{"
        + " artifactId=" + artifactId
        + ", namespace=" + namespace
        + ", locationURI=" + locationURI
        + ", location=" + location
        + '}';
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
    return Objects.hash(namespace, artifactId);
  }

  @Override
  public int compareTo(ArtifactDescriptor other) {
    int code = getNamespace().compareTo(other.getNamespace());
    if (code != 0) {
      return code;
    }
    return getArtifactId().compareTo(other.getArtifactId());
  }
}
