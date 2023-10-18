/*
 * Copyright © 2020 Cask Data, Inc.
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
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.common.ArtifactRepositoryReader;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.proto.artifact.artifact.ArtifactDetail;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Implementation for {@link ArtifactRepositoryReader} to fetch artifact metadata directly from
 * local {@link ArtifactStore}
 */
public class LocalArtifactRepositoryReader implements ArtifactRepositoryReader {

  private final ArtifactStore artifactStore;

  @Inject
  public LocalArtifactRepositoryReader(ArtifactStore artifactStore) {
    this.artifactStore = artifactStore;
  }

  @Override
  public ArtifactDetail getArtifact(Id.Artifact artifactId) throws Exception {
    return artifactStore.getArtifact(artifactId);
  }

  @Override
  public InputStream newInputStream(Id.Artifact artifactId) throws IOException, NotFoundException {
    return artifactStore.getArtifact(artifactId).getDescriptor().getLocation().getInputStream();
  }

  @Override
  public List<ArtifactDetail> getArtifactDetails(ArtifactRange range, int limit,
      ArtifactSortOrder order) {
    return artifactStore.getArtifacts(range, limit, order);
  }
}
