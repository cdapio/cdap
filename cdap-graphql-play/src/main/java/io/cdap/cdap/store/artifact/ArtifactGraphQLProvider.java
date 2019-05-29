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

package io.cdap.cdap.store.artifact;

import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.TypeRuntimeWiring;
import io.cdap.cdap.graphql.provider.AbstractGraphQLProvider;
import io.cdap.cdap.graphql.schema.Types;
import io.cdap.cdap.store.artifact.datafetchers.ArtifactDataFetchers;
import io.cdap.cdap.store.artifact.schema.ArtifactFields;
import io.cdap.cdap.store.artifact.schema.ArtifactTypes;

public class ArtifactGraphQLProvider extends AbstractGraphQLProvider {

  private final ArtifactDataFetchers artifactDataFetchers;

  public ArtifactGraphQLProvider(String schemaDefinitionFile, ArtifactDataFetchers artifactDataFetchers) {
    super(schemaDefinitionFile);

    this.artifactDataFetchers = artifactDataFetchers;
  }

  @Override
  protected RuntimeWiring buildWiring() {
    return RuntimeWiring.newRuntimeWiring()
      .type(getQueryTypeRuntimeWiring())
      .type(getArtifactDetailRuntimeWiring())
      .build();
  }

  private TypeRuntimeWiring getQueryTypeRuntimeWiring() {
    return TypeRuntimeWiring.newTypeWiring(Types.QUERY)
      .dataFetcher(ArtifactFields.ARTIFACTS, artifactDataFetchers.getArtifactsDataFetcher())
      .dataFetcher(ArtifactFields.ARTIFACT_DETAIL, artifactDataFetchers.getArtifactDetailDataFetcher())
      .build();
  }

  private TypeRuntimeWiring getArtifactDetailRuntimeWiring() {
    return TypeRuntimeWiring.newTypeWiring(ArtifactTypes.ARTIFACT_DETAIL)
      .dataFetcher(ArtifactFields.DESCRIPTOR, artifactDataFetchers.getArtifactDescriptorDataFetcher())
      .build();
  }
}