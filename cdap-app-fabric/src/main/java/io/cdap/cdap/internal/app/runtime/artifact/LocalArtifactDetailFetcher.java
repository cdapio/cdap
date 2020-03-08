package io.cdap.cdap.internal.app.runtime.artifact;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.proto.id.ArtifactId;

public class LocalArtifactDetailFetcher implements ArtifactDetailFetcher {
  private final ArtifactRepository noAuthArtifactRepository;

  @Inject
  public LocalArtifactDetailFetcher(
     @Named(AppFabricServiceRuntimeModule.NOAUTH_ARTIFACT_REPO)
       ArtifactRepository noAuthArtifactRepository) {
    this.noAuthArtifactRepository = noAuthArtifactRepository;
  }

  @Override
  public ArtifactDetail get(ArtifactId artifactId) throws Exception {
    return noAuthArtifactRepository.getArtifact(Id.Artifact.fromEntityId(artifactId));
  }
}
