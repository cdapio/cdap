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

package co.cask.cdap.internal.app.runtime.artifact;

import com.google.common.collect.ImmutableSet;

import java.util.Objects;
import java.util.Set;

/**
 * Metadata about an artifact, such as what plugins are contained in the artifact, and what other artifacts can use
 * the plugins in this artifact. For example, we could have an etl-batch-lib artifact that contains
 * 20 different plugins that are meant to be used by the application contained in the etl-batch artifact.
 * In this case, the artifact meta for etl-batch-lib would contain details about each of those 20 plugins, as well
 * as information about which versions of the etl-batch artifact can use the plugins it contains.
 */
public class ArtifactMeta {
  private final ArtifactClasses classes;
  // can't call this 'extends' since that's a reserved keyword
  private final Set<ArtifactRange> usableBy;

  public ArtifactMeta(ArtifactClasses classes) {
    this(classes, ImmutableSet.<ArtifactRange>of());
  }

  public ArtifactMeta(ArtifactClasses classes, Set<ArtifactRange> usableBy) {
    this.classes = classes;
    this.usableBy = usableBy;
  }

  public ArtifactClasses getClasses() {
    return classes;
  }

  public Set<ArtifactRange> getUsableBy() {
    return usableBy;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArtifactMeta that = (ArtifactMeta) o;

    return Objects.equals(classes, that.classes) && Objects.equals(usableBy, that.usableBy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(classes, usableBy);
  }

  @Override
  public String toString() {
    return "ArtifactMeta{" +
      "classes=" + classes +
      ", usableBy=" + usableBy +
      '}';
  }
}
