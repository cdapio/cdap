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
import io.cdap.cdap.graphql.store.artifact.datafetchers.ApplicationsDataFetcher;
import io.cdap.cdap.graphql.store.artifact.datafetchers.LocationDataFetcher;
import io.cdap.cdap.graphql.store.artifact.datafetchers.PluginsDataFetcher;
import io.cdap.cdap.graphql.store.artifact.schema.ArtifactFields;
import io.cdap.cdap.graphql.store.artifact.schema.ArtifactTypes;
import io.cdap.cdap.graphql.typeruntimewiring.CDAPTypeRuntimeWiring;

/**
 * TODO
 */
public class ArtifactTypeRuntimeWiring implements CDAPTypeRuntimeWiring {

  private final LocationDataFetcher locationDataFetcher;
  private final PluginsDataFetcher pluginsDataFetcher;
  private final ApplicationsDataFetcher applicationsDataFetcher;

  /**
   * TODO
   */
  @Inject
  ArtifactTypeRuntimeWiring(LocationDataFetcher locationDataFetcher,
                            PluginsDataFetcher pluginsDataFetcher,
                            ApplicationsDataFetcher applicationsDataFetcher) {
    this.locationDataFetcher = locationDataFetcher;
    this.pluginsDataFetcher = pluginsDataFetcher;
    this.applicationsDataFetcher = applicationsDataFetcher;
  }

  /**
   * TODO
   */
  @Override
  public TypeRuntimeWiring getTypeRuntimeWiring() {
    return TypeRuntimeWiring.newTypeWiring(ArtifactTypes.ARTIFACT)
      .dataFetcher(ArtifactFields.LOCATION, locationDataFetcher.getLocationDataFetcher())
      .dataFetcher(ArtifactFields.PLUGINS, pluginsDataFetcher.getPluginsDataFetcher())
      .dataFetcher(ArtifactFields.APPLICATIONS, applicationsDataFetcher.getApplicationsDataFetcher())
      .build();
  }

}
