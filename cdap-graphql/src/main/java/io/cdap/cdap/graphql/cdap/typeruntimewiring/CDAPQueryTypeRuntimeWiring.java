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

package io.cdap.cdap.graphql.cdap.typeruntimewiring;

import com.google.inject.Inject;
import graphql.schema.idl.TypeRuntimeWiring;
import io.cdap.cdap.graphql.cdap.schema.GraphQLFields;
import io.cdap.cdap.graphql.cdap.schema.GraphQLTypes;
import io.cdap.cdap.graphql.store.application.datafetchers.ApplicationDataFetcher;
import io.cdap.cdap.graphql.store.application.schema.ApplicationFields;
import io.cdap.cdap.graphql.store.artifact.datafetchers.ArtifactDataFetcher;
import io.cdap.cdap.graphql.store.artifact.schema.ArtifactFields;
import io.cdap.cdap.graphql.typeruntimewiring.CDAPTypeRuntimeWiring;

import java.sql.Timestamp;

/**
 * Top level CDAP query type runtime wiring. Mainly use to integrate multiple schemas in the server
 */
public class CDAPQueryTypeRuntimeWiring implements CDAPTypeRuntimeWiring {

  private final ApplicationDataFetcher applicationDataFetcher;
  private final ArtifactDataFetcher artifactDataFetcher;

  @Inject
  public CDAPQueryTypeRuntimeWiring(ApplicationDataFetcher applicationDataFetcher,
                                    ArtifactDataFetcher artifactDataFetcher) {
    this.applicationDataFetcher = applicationDataFetcher;
    this.artifactDataFetcher = artifactDataFetcher;
  }

  @Override
  public TypeRuntimeWiring getTypeRuntimeWiring() {
    return TypeRuntimeWiring.newTypeWiring(GraphQLTypes.CDAP_QUERY)
      .dataFetcher(GraphQLFields.TIMESTAMP, dataFetchingEnvironment -> new Timestamp(System.currentTimeMillis()))
      .dataFetcher(ApplicationFields.APPLICATIONS, applicationDataFetcher.getApplicationRecordsDataFetcher())
      .dataFetcher(ApplicationFields.APPLICATION, applicationDataFetcher.getApplicationDetailFromQueryDataFetcher())
      .dataFetcher(ArtifactFields.ARTIFACTS, artifactDataFetcher.getArtifactsDataFetcher())
      .dataFetcher(ArtifactFields.ARTIFACT, artifactDataFetcher.getArtifactDataFetcher())
      .build();
  }

}
