/*
 * Copyright © 2021 Cask Data, Inc.
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

import com.google.inject.Inject;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.ArtifactRepositoryReader;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerClient;
import io.cdap.cdap.proto.artifact.artifact.ArtifactDescriptor;
import io.cdap.cdap.proto.id.ArtifactId;
import java.io.IOException;
import org.apache.twill.filesystem.Location;

/**
 * {@link RemoteArtifactRepositoryWithLocalization} is an extension of {@link
 * RemoteArtifactRepository} that localizes artifacts and use their local locations in returned
 * values.
 *
 * This implementation uses {@link ArtifactLocalizerClient} to download and cache artifacts on the
 * local file system.
 */
public class RemoteArtifactRepositoryWithLocalization extends RemoteArtifactRepository {

  private final ArtifactLocalizerClient artifactLocalizerClient;

  @Inject
  RemoteArtifactRepositoryWithLocalization(CConfiguration cConf,
      ArtifactRepositoryReader artifactRepositoryReader,
      ArtifactLocalizerClient artifactLocalizerClient) {
    super(cConf, artifactRepositoryReader);
    this.artifactLocalizerClient = artifactLocalizerClient;
  }

  @Override
  protected Location getArtifactLocation(ArtifactDescriptor descriptor) throws IOException {
    ArtifactId artifactId = new ArtifactId(descriptor.getNamespace(),
        descriptor.getArtifactId().getName(),
        descriptor.getArtifactId().getVersion().getVersion());
    try {
      return Locations.toLocation(artifactLocalizerClient.getArtifactLocation(artifactId));
    } catch (ArtifactNotFoundException e) {
      throw new IOException(String.format("Artifact %s is not found", artifactId), e);
    }
  }
}
