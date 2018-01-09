/*
 * Copyright © 2018 Cask Data, Inc.
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

import co.cask.cdap.api.artifact.ArtifactInfo;
import co.cask.cdap.api.artifact.ArtifactManager;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of {@link ArtifactManager} that talks to {@link ArtifactRepository} directly.
 * This is used in unit-test and SDK.
 */
public final class LocalArtifactManager extends AbstractArtifactManager {

  private final ArtifactRepository artifactRepository;
  private final NamespaceId namespaceId;
  private final RetryStrategy retryStrategy;

  @Inject
  LocalArtifactManager(CConfiguration cConf,
                       ArtifactRepository artifactRepository,
                       @Assisted NamespaceId namespaceId,
                       @Assisted RetryStrategy retryStrategy) {
    super(cConf);
    this.artifactRepository = artifactRepository;
    this.namespaceId = namespaceId;
    this.retryStrategy = retryStrategy;
  }

  @Override
  public List<ArtifactInfo> listArtifacts() throws IOException {
    return Retries.callWithRetries(() -> {
      try {
        List<ArtifactInfo> result = new ArrayList<>(artifactRepository.getArtifactsInfo(namespaceId));
        result.addAll(artifactRepository.getArtifactsInfo(NamespaceId.SYSTEM));
        return result;
      } catch (IOException | RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException(e);
      }
    }, retryStrategy);
  }

  @Override
  protected Location getArtifactLocation(ArtifactInfo artifactInfo) throws IOException {
    NamespaceId namespace = ArtifactScope.SYSTEM.equals(artifactInfo.getScope()) ? NamespaceId.SYSTEM : namespaceId;
    ArtifactId artifactId = namespace.artifact(artifactInfo.getName(), artifactInfo.getVersion());

    return Retries.callWithRetries(() -> {
      try {
        ArtifactDetail artifactDetail = artifactRepository.getArtifact(artifactId.toId());
        return artifactDetail.getDescriptor().getLocation();
      } catch (IOException | RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException(e);
      }
    }, retryStrategy);
  }
}
