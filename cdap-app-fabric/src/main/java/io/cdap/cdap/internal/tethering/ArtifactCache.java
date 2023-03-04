/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.tethering;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.internal.app.worker.sidecar.AbstractArtifactLocalizer;
import io.cdap.cdap.proto.id.ArtifactId;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

/**
 * ArtifactCache is responsible for fetching and caching artifacts from tethered peers. The HTTP
 * endpoints are defined in {@link ArtifactCacheHttpHandlerInternal}.
 *
 * Artifacts will be cached using the following file structure:
 * {@code /DATA_DIRECTORY/peers/{peer-name}/artifacts/{namespace}/{artifact-name}/{artifact-version}/{last-modified-timestamp}.jar}
 */
public class ArtifactCache extends AbstractArtifactLocalizer {

  @Inject
  ArtifactCache(CConfiguration cConf) {
    super(cConf.get(Constants.ArtifactCache.LOCAL_DATA_DIR),
        RetryStrategies.fromConfiguration(cConf, Constants.Service.ARTIFACT_CACHE + "."));
  }

  /**
   * Gets the location on the local filesystem for the given artifact. This method handles fetching
   * the artifact as well as caching it.
   *
   * @param artifactId The ArtifactId of the artifact to fetch
   * @param peer The name of the tethered peer
   * @param remoteClient The remote client used to connect to appfabric on the peer
   * @return The Local Location for this artifact
   * @throws IOException if there was an exception while fetching or caching the artifact
   * @throws Exception if there was an unexpected error
   */
  public File getArtifact(ArtifactId artifactId, String peer, RemoteClient remoteClient)
      throws Exception {
    File artifactDir = getArtifactDirLocation(artifactId, peer);
    return Retries.callWithRetries(() -> fetchArtifact(artifactId, remoteClient, artifactDir),
        retryStrategy);
  }

  /**
   * Returns a {@link File} representing the cache directory jars for the given artifact belonging
   * to a tethered peer. The file path is:
   * {@code /DATA_DIRECTORY/peers/<peer-name>/</peer-name>artifacts/<namespace>/<artifact-name>/<artifact-version>/}
   */
  private File getArtifactDirLocation(ArtifactId artifactId, String peerName) {
    return Paths.get(dataDir, "peers", peerName, "artifacts", artifactId.getNamespace(),
        artifactId.getArtifact(), artifactId.getVersion()).toFile();

  }
}
