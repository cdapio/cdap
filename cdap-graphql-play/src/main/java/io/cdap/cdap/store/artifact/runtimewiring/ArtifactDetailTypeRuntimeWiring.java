/*
 *
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.store.artifact.runtimewiring;

import com.google.inject.Inject;
import graphql.schema.idl.TypeRuntimeWiring;
import io.cdap.cdap.store.artifact.datafetchers.ArtifactDescriptorDataFetcher;
import io.cdap.cdap.store.artifact.schema.ArtifactFields;
import io.cdap.cdap.store.artifact.schema.ArtifactTypes;

public class ArtifactDetailTypeRuntimeWiring {

  private final ArtifactDescriptorDataFetcher artifactDescriptorDataFetcher;

  @Inject
  ArtifactDetailTypeRuntimeWiring(ArtifactDescriptorDataFetcher artifactDescriptorDataFetcher) {
    this.artifactDescriptorDataFetcher = artifactDescriptorDataFetcher;
  }

  public TypeRuntimeWiring getArtifactDetailRuntimeWiring() {
    return TypeRuntimeWiring.newTypeWiring(ArtifactTypes.ARTIFACT_DETAIL)
      .dataFetcher(ArtifactFields.DESCRIPTOR, artifactDescriptorDataFetcher.getArtifactDescriptorDataFetcher())
      .build();
  }

}
