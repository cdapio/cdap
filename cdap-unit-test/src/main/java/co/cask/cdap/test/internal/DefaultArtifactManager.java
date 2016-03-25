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

package co.cask.cdap.test.internal;

import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.proto.id.NamespacedArtifactId;
import co.cask.cdap.test.ArtifactManager;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import java.util.Map;

/**
 * {@link ArtifactManager} for use in unit tests
 */
public class DefaultArtifactManager implements ArtifactManager {
  private final ArtifactRepository artifactRepository;
  private final NamespacedArtifactId artifactId;

  @Inject
  DefaultArtifactManager(ArtifactRepository artifactRepository,
                         @Assisted("artifactId") NamespacedArtifactId artifactId) {
    this.artifactRepository = artifactRepository;
    this.artifactId = artifactId;
  }

  @Override
  public void writeProperties(Map<String, String> properties) throws Exception {
    artifactRepository.writeArtifactProperties(artifactId.toId(), properties);
  }

  @Override
  public void removeProperties() throws Exception {
    artifactRepository.deleteArtifactProperties(artifactId.toId());
  }

  @Override
  public void delete() throws Exception {
    artifactRepository.deleteArtifact(artifactId.toId());
  }
}
