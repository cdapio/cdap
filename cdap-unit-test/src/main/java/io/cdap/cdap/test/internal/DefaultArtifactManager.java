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

package io.cdap.cdap.test.internal;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.test.ArtifactManager;

import java.util.Map;

/**
 * {@link ArtifactManager} for use in unit tests
 */
public class DefaultArtifactManager implements ArtifactManager {
  private final ArtifactRepository artifactRepository;
  private final ArtifactId artifactId;

  @Inject
  DefaultArtifactManager(ArtifactRepository artifactRepository,
                         @Assisted("artifactId") ArtifactId artifactId) {
    this.artifactRepository = artifactRepository;
    this.artifactId = artifactId;
  }

  @Override
  public void writeProperties(Map<String, String> properties) throws Exception {
    artifactRepository.writeArtifactProperties(Id.Artifact.fromEntityId(artifactId), properties);
  }

  @Override
  public void removeProperties() throws Exception {
    artifactRepository.deleteArtifactProperties(Id.Artifact.fromEntityId(artifactId));
  }

  @Override
  public void delete() throws Exception {
    artifactRepository.deleteArtifact(Id.Artifact.fromEntityId(artifactId));
  }
}
