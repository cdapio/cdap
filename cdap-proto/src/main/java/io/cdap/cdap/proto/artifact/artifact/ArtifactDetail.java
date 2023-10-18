/*
 * Copyright © 2015 Cask Data, Inc.
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

package io.cdap.cdap.proto.artifact.artifact;

import java.util.Objects;

/**
 * Details about an artifact, including info about the artifact itself and metadata about the
 * contents of the artifact.
 */
public class ArtifactDetail {

  private final ArtifactDescriptor descriptor;
  private final ArtifactMeta meta;

  public ArtifactDetail(ArtifactDescriptor descriptor, ArtifactMeta meta) {
    this.descriptor = descriptor;
    this.meta = meta;
  }

  public ArtifactDescriptor getDescriptor() {
    return descriptor;
  }

  public ArtifactMeta getMeta() {
    return meta;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArtifactDetail that = (ArtifactDetail) o;
    return Objects.equals(descriptor, that.descriptor)
        && Objects.equals(meta, that.meta);
  }

  @Override
  public int hashCode() {
    return Objects.hash(descriptor, meta);
  }
}
