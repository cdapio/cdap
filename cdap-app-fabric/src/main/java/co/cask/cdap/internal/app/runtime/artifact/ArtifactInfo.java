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

import co.cask.cdap.proto.Id;
import com.google.common.base.Objects;
import org.apache.twill.filesystem.Location;

/**
 * Information about the artifact itself, but nothing about the contents of the artifact.
 */
public class ArtifactInfo {
  private final Id.Artifact id;
  private final Location location;

  public ArtifactInfo(Id.Artifact id, Location location) {
    this.id = id;
    this.location = location;
  }

  public Id.Artifact getId() {
    return id;
  }

  public Location getLocation() {
    return location;
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

    return Objects.equal(id, that.id) && Objects.equal(location, that.location);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, location);
  }
}
