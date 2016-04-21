/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.entity.EntityExistenceVerifier;
import co.cask.cdap.proto.id.ArtifactId;
import com.google.common.base.Throwables;
import com.google.inject.Inject;

import java.io.IOException;

/**
 * {@link EntityExistenceVerifier} for {@link ArtifactId artifacts}.
 */
public class ArtifactExistenceVerifier implements EntityExistenceVerifier<ArtifactId> {

  private final ArtifactStore artifactStore;

  @Inject
  ArtifactExistenceVerifier(ArtifactStore artifactStore) {
    this.artifactStore = artifactStore;
  }

  @Override
  public void ensureExists(ArtifactId artifactId) throws ArtifactNotFoundException {
    try {
      artifactStore.getArtifact(artifactId.toId());
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
