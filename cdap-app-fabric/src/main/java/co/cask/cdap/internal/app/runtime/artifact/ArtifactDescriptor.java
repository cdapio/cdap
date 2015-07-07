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

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.internal.artifact.ArtifactVersion;
import org.apache.twill.filesystem.Location;

import java.util.Objects;

/**
 * Uniquely describes an artifact. Artifact descriptors are ordered by whether or not they are system artifacts,
 * then by name, and finally by version.
 * TODO: move this into cdap-api once everything is ready
 */
@Beta
public final class ArtifactDescriptor implements Comparable<ArtifactDescriptor> {
  private final String name;
  private final ArtifactVersion version;
  private final boolean isSystem;
  private final Location location;

  public ArtifactDescriptor(String name, ArtifactVersion version, boolean isSystem, Location location) {
    this.name = name;
    this.version = version;
    this.isSystem = isSystem;
    this.location = location;
  }

  public String getName() {
    return name;
  }

  public ArtifactVersion getVersion() {
    return version;
  }

  public boolean isSystem() {
    return isSystem;
  }

  public Location getLocation() {
    return location;
  }

  @Override
  public String toString() {
    return "ArtifactDescriptor{" +
      "name='" + name + '\'' +
      ", version=" + version +
      ", isSystem=" + isSystem +
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
    return Objects.hash(name, version, isSystem);
  }

  @Override
  public int compareTo(ArtifactDescriptor other) {
    // system artifacts are 'less than' other artifacts
    if (isSystem != other.isSystem) {
      return isSystem ? -1 : 1;
    }
    int cmp = name.compareTo(other.name);
    if (cmp != 0) {
      return cmp;
    }
    return version.compareTo(other.version);
  }
}
