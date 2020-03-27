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
import io.cdap.cdap.common.id.Id;

/**
 * Implementation for {@link ArtifactRepositoryReader} to fetch artifact metadata directly from local
 * {@link ArtifactStore}
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
}
