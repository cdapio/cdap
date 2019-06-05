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

import graphql.schema.idl.TypeRuntimeWiring;
import io.cdap.cdap.graphql.store.artifact.datafetchers.ApplicationDataFetcher;
import io.cdap.cdap.graphql.store.artifact.datafetchers.LocationDataFetcher;
import io.cdap.cdap.graphql.store.artifact.datafetchers.PluginDataFetcher;
import io.cdap.cdap.graphql.store.artifact.schema.ArtifactFields;
import io.cdap.cdap.graphql.store.artifact.schema.ArtifactTypes;
import io.cdap.cdap.graphql.typeruntimewiring.CDAPTypeRuntimeWiring;

/**
 * Artifact type runtime wiring. Registers the data fetchers for the Namespace type.
 */
public class ArtifactTypeRuntimeWiring implements CDAPTypeRuntimeWiring {

  private static final ArtifactTypeRuntimeWiring INSTANCE = new ArtifactTypeRuntimeWiring();

  private final LocationDataFetcher locationDataFetcher;
  private final PluginDataFetcher pluginDataFetcher;
  private final ApplicationDataFetcher applicationDataFetcher;
  // private final NamespaceDataFetcher namespaceDataFetcher;

  private ArtifactTypeRuntimeWiring() {
    this.locationDataFetcher = LocationDataFetcher.getInstance();
    this.pluginDataFetcher = PluginDataFetcher.getInstance();
    this.applicationDataFetcher = ApplicationDataFetcher.getInstance();
    // this.namespaceDataFetcher = NamespaceDataFetcher.getInstance();
  }

  public static CDAPTypeRuntimeWiring getInstance() {
    return INSTANCE;
  }

  @Override
  public TypeRuntimeWiring getTypeRuntimeWiring() {
    return TypeRuntimeWiring.newTypeWiring(ArtifactTypes.ARTIFACT)
      .dataFetcher(ArtifactFields.LOCATION, locationDataFetcher.getLocationDataFetcher())
      .dataFetcher(ArtifactFields.PLUGINS, pluginDataFetcher.getPluginsDataFetcher())
      .dataFetcher(ArtifactFields.APPLICATIONS, applicationDataFetcher.getApplicationsDataFetcher())
      // .dataFetcher(GraphQLFields.NAMESPACE, namespaceDataFetcher.getNamespaceFromSourceDataFetcher())
      .build();
  }

}
