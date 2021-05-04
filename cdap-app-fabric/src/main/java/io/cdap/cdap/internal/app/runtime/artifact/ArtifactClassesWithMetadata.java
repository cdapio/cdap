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
 *
 */

package io.cdap.cdap.internal.app.runtime.artifact;

import io.cdap.cdap.api.artifact.ArtifactClasses;
import io.cdap.cdap.spi.metadata.MetadataMutation;

import java.util.List;
import java.util.Objects;

/**
 * Artifact classes along with metadata
 */
public class ArtifactClassesWithMetadata {
  private final ArtifactClasses artifactClasses;
  private final List<MetadataMutation> mutations;

  public ArtifactClassesWithMetadata(ArtifactClasses artifactClasses, List<MetadataMutation> mutations) {
    this.artifactClasses = artifactClasses;
    this.mutations = mutations;
  }

  public ArtifactClasses getArtifactClasses() {
    return artifactClasses;
  }

  public List<MetadataMutation> getMutations() {
    return mutations;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArtifactClassesWithMetadata that = (ArtifactClassesWithMetadata) o;
    return Objects.equals(artifactClasses, that.artifactClasses) &&
             Objects.equals(mutations, that.mutations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(artifactClasses, mutations);
  }
}
