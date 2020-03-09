package io.cdap.cdap.internal.app.runtime.artifact;

import io.cdap.cdap.proto.id.ArtifactId;

public interface ArtifactDetailFetcher {
  ArtifactDetail get(ArtifactId artifactId) throws Exception;
}
