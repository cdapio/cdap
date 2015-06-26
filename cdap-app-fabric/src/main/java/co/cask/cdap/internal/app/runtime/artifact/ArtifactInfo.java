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
import org.apache.twill.filesystem.Location;

/**
 * Information about an artifact.
 */
public class ArtifactInfo {
  private final Id.Artifact id;
  private final Location location;
  private final ArtifactMeta meta;

  public ArtifactInfo(Id.Artifact id, Location location, ArtifactMeta meta) {
    this.id = id;
    this.location = location;
    this.meta = meta;
  }

  public Id.Artifact getId() {
    return id;
  }

  public Location getLocation() {
    return location;
  }

  public ArtifactMeta getMeta() {
    return meta;
  }
}
