/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.artifact;

import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Interface for finding plugin.
 */
public interface ArtifactFinder {

  /**
   * Finds the location of an artifact.
   *
   * @param artifactId the artifact id
   * @return the location of the artifact.
   * @throws ArtifactNotFoundException if no plugin can be found
   */
  Location getArtifactLocation(ArtifactId artifactId)
    throws ArtifactNotFoundException, IOException, UnauthorizedException;

  /**
   * Finds the locations of multiple artifacts. If an artifact could not be found, the artifact will not be
   * a key in the returned map.
   *
   * @param artifactIds the ids of the artifacts to get
   * @return map of artifact id to artifact location
   * @throws IOException if there was an exception fetching the locations
   */
  default Map<ArtifactId, Location> getArtifactLocations(Collection<ArtifactId> artifactIds)
    throws IOException, UnauthorizedException {
    Map<ArtifactId, Location> locations = new HashMap<>(artifactIds.size());
    for (ArtifactId id : artifactIds) {
      try {
        locations.put(id, getArtifactLocation(id));
      } catch (ArtifactNotFoundException e) {
        // don't add to returned locations
      }
    }
    return locations;
  }
}
