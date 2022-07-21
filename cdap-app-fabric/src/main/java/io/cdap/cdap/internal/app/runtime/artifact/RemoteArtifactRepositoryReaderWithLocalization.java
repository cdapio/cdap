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
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerClient;
import io.cdap.cdap.proto.id.ArtifactId;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;

/**
 * {@link RemoteArtifactRepositoryReaderWithLocalization} is an extension of {@link RemoteArtifactRepositoryReader}
 * that localizes artifacts and use their local locations in returned value.
 *
 * This implementation uses {@link ArtifactLocalizerClient} to fetch and cache the given artifact on the local
 * file system.
 */
public class RemoteArtifactRepositoryReaderWithLocalization extends RemoteArtifactRepositoryReader {
  private final ArtifactLocalizerClient artifactLocalizerClient;

  @Inject
  RemoteArtifactRepositoryReaderWithLocalization(LocationFactory locationFactory,
                                                 RemoteClientFactory remoteClientFactory,
                                                 ArtifactLocalizerClient artifactLocalizerClient) {
    super(locationFactory, remoteClientFactory);
    this.artifactLocalizerClient = artifactLocalizerClient;
  }

  @Override
  protected Location getArtifactLocation(ArtifactDescriptor descriptor) throws IOException, ArtifactNotFoundException {
    return Locations.toLocation(artifactLocalizerClient.getArtifactLocation(
      new ArtifactId(descriptor.getNamespace(),
                     descriptor.getArtifactId().getName(),
                     descriptor.getArtifactId().getVersion().getVersion())));
  }
}

