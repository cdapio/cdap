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

package io.cdap.cdap.internal.app.runtime.artifact;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

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
    return listArtifacts(namespaceId);
  }

  @Override
  public List<ArtifactInfo> listArtifacts(String namespace) throws IOException {
    return listArtifacts(new NamespaceId(namespace));
  }

  private List<ArtifactInfo> listArtifacts(NamespaceId namespaceId) throws IOException {
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
  public Location getArtifactLocation(ArtifactSummary artifactSummary,
                                      @Nullable String artifactNamespace) throws IOException {
    NamespaceId namespace;
    if (ArtifactScope.SYSTEM.equals(artifactSummary.getScope())) {
      namespace = NamespaceId.SYSTEM;
    } else if (artifactNamespace != null) {
      namespace = new NamespaceId(artifactNamespace);
    } else {
      namespace = namespaceId;
    }
    ArtifactId artifactId = namespace.artifact(artifactSummary.getName(), artifactSummary.getVersion());

    return Retries.callWithRetries(() -> {
      try {
        ArtifactDetail artifactDetail = artifactRepository.getArtifact(Id.Artifact.fromEntityId(artifactId));
        return artifactDetail.getDescriptor().getLocation();
      } catch (IOException | RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException(e);
      }
    }, retryStrategy);
  }
}
