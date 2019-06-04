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

package io.cdap.cdap.graphql.cdap.provider;

import graphql.schema.idl.RuntimeWiring;
import io.cdap.cdap.graphql.cdap.runtimewiring.CDAPQueryTypeRuntimeWiring;
import io.cdap.cdap.graphql.provider.AbstractGraphQLProvider;
import io.cdap.cdap.graphql.store.application.runtimewiring.ApplicationQueryTypeRuntimeWiring;
import io.cdap.cdap.graphql.store.artifact.runtimewiring.ArtifactQueryTypeRuntimeWiring;
import io.cdap.cdap.graphql.store.artifact.runtimewiring.ArtifactTypeRuntimeWiring;
import io.cdap.cdap.graphql.store.namespace.runtimewiring.NamespaceQueryTypeRuntimeWiring;
import io.cdap.cdap.graphql.store.namespace.runtimewiring.NamespaceTypeRuntimeWiring;

import java.util.List;

/**
 * Implementation of an {@link AbstractGraphQLProvider} to create the CDAP GraphQL server
 */
public class CDAPGraphQLProvider extends AbstractGraphQLProvider {

  private final CDAPQueryTypeRuntimeWiring cdapQueryTypeRuntimeWiring;
  private final ArtifactQueryTypeRuntimeWiring artifactQueryTypeRuntimeWiring;
  private final ArtifactTypeRuntimeWiring artifactTypeRuntimeWiring;
  private final NamespaceQueryTypeRuntimeWiring namespaceQueryTypeRuntimeWiring;
  private final NamespaceTypeRuntimeWiring namespaceTypeRuntimeWiring;
  private final ApplicationQueryTypeRuntimeWiring applicationQueryTypeRuntimeWiring;

  public CDAPGraphQLProvider(List<String> schemaDefinitionFiles) {
    super(schemaDefinitionFiles);

    this.cdapQueryTypeRuntimeWiring = new CDAPQueryTypeRuntimeWiring();
    this.artifactQueryTypeRuntimeWiring = new ArtifactQueryTypeRuntimeWiring();
    this.artifactTypeRuntimeWiring = new ArtifactTypeRuntimeWiring();
    this.namespaceQueryTypeRuntimeWiring = new NamespaceQueryTypeRuntimeWiring();
    this.namespaceTypeRuntimeWiring = new NamespaceTypeRuntimeWiring();
    this.applicationQueryTypeRuntimeWiring = new ApplicationQueryTypeRuntimeWiring();
  }

  @Override
  protected RuntimeWiring buildWiring() {
    return RuntimeWiring.newRuntimeWiring()
      .type(cdapQueryTypeRuntimeWiring.getTypeRuntimeWiring())
      .type(artifactQueryTypeRuntimeWiring.getTypeRuntimeWiring())
      .type(artifactTypeRuntimeWiring.getTypeRuntimeWiring())
      .type(namespaceQueryTypeRuntimeWiring.getTypeRuntimeWiring())
      .type(namespaceTypeRuntimeWiring.getTypeRuntimeWiring())
      .type(applicationQueryTypeRuntimeWiring.getTypeRuntimeWiring())
      .build();
  }

}
