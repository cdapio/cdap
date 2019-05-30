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
import io.cdap.cdap.graphql.provider.AbstractGraphQLProvider;
import io.cdap.cdap.store.artifact.runtimewiring.ArtifactDescriptorTypeRuntimeWiring;
import io.cdap.cdap.store.artifact.runtimewiring.ArtifactDetailTypeRuntimeWiring;
import io.cdap.cdap.store.artifact.runtimewiring.QueryTypeRuntimeWiring;

/**
 * TODO
 */
public class ArtifactGraphQLProvider extends AbstractGraphQLProvider {

  private final QueryTypeRuntimeWiring queryTypeRuntimeWiring;
  private final ArtifactDetailTypeRuntimeWiring artifactDetailTypeRuntimeWiring;
  private final ArtifactDescriptorTypeRuntimeWiring artifactDescriptorTypeRuntimeWiring;

  /**
   * TODO
   */
  public ArtifactGraphQLProvider(String schemaDefinitionFile, QueryTypeRuntimeWiring queryTypeRuntimeWiring,
                                 ArtifactDetailTypeRuntimeWiring artifactDetailTypeRuntimeWiring,
                                 ArtifactDescriptorTypeRuntimeWiring artifactDescriptorTypeRuntimeWiring) {
    super(schemaDefinitionFile);

    this.queryTypeRuntimeWiring = queryTypeRuntimeWiring;
    this.artifactDetailTypeRuntimeWiring = artifactDetailTypeRuntimeWiring;
    this.artifactDescriptorTypeRuntimeWiring = artifactDescriptorTypeRuntimeWiring;
  }

  @Override
  protected RuntimeWiring buildWiring() {
    return RuntimeWiring.newRuntimeWiring()
      .type(queryTypeRuntimeWiring.getTypeRuntimeWiring())
      .type(artifactDetailTypeRuntimeWiring.getTypeRuntimeWiring())
      .type(artifactDescriptorTypeRuntimeWiring.getTypeRuntimeWiring())
      .build();
  }

}
