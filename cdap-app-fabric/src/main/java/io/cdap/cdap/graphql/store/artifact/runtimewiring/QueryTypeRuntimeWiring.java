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

package io.cdap.cdap.graphql.store.artifact.runtimewiring;

import com.google.inject.Inject;
import graphql.schema.idl.TypeRuntimeWiring;
import io.cdap.cdap.graphql.schema.Types;
import io.cdap.cdap.graphql.store.artifact.datafetchers.ArtifactDataFetcher;
import io.cdap.cdap.graphql.store.artifact.schema.ArtifactFields;
import io.cdap.cdap.graphql.typeruntimewiring.CDAPTypeRuntimeWiring;

/**
 * TODO
 */
public class QueryTypeRuntimeWiring implements CDAPTypeRuntimeWiring {

  private final ArtifactDataFetcher artifactDataFetcher;

  /**
   * TODO
   */
  @Inject
  QueryTypeRuntimeWiring(ArtifactDataFetcher artifactDataFetcher) {
    this.artifactDataFetcher = artifactDataFetcher;
  }

  /**
   * TODO
   */
  @Override
  public TypeRuntimeWiring getTypeRuntimeWiring() {
    return TypeRuntimeWiring.newTypeWiring(Types.QUERY)
      .dataFetcher(ArtifactFields.ARTIFACTS, artifactDataFetcher.getArtifactsDataFetcher())
      .build();
  }

}
