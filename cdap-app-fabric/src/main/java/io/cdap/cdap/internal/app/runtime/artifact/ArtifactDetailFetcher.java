package io.cdap.cdap.internal.app.runtime.artifact;

import io.cdap.cdap.proto.id.ArtifactId;

public interface ArtifactDetailFetcher {
    /**
   * Get preferences for the given identify
   * @param entityId the id of the entity to fetch preferences for
   * @param resolved true if resolved properties are desired.
   * @return the detail of preferences
   * @throws IOException if failed to get preferences
   * @throws NotFoundException if the given entity doesn't exist.
   */
  ArtifactDetail get(ArtifactId artifactId) throws Exception;
}
