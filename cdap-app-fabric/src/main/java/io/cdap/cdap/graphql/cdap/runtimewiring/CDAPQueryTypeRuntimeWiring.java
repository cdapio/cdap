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

package io.cdap.cdap.graphql.cdap.runtimewiring;

import graphql.schema.idl.TypeRuntimeWiring;
import io.cdap.cdap.graphql.cdap.schema.GraphQLFields;
import io.cdap.cdap.graphql.cdap.schema.GraphQLTypes;
import io.cdap.cdap.graphql.typeruntimewiring.CDAPTypeRuntimeWiring;

import java.sql.Timestamp;

/**
 * Top level CDAP query type runtime wiring. Mainly use to integrate multiple schemas in the server
 */
public class CDAPQueryTypeRuntimeWiring implements CDAPTypeRuntimeWiring {

  private static final CDAPQueryTypeRuntimeWiring INSTANCE = new CDAPQueryTypeRuntimeWiring();

  private CDAPQueryTypeRuntimeWiring() {

  }

  public static CDAPTypeRuntimeWiring getInstance() {
    return INSTANCE;
  }

  @Override
  public TypeRuntimeWiring getTypeRuntimeWiring() {
    return TypeRuntimeWiring.newTypeWiring(GraphQLTypes.CDAP_QUERY)
      .dataFetcher(GraphQLFields.TIMESTAMP, dataFetchingEnvironment -> new Timestamp(System.currentTimeMillis()))
      .dataFetcher(GraphQLFields.ARTIFACT, dataFetchingEnvironment -> dataFetchingEnvironment)
      .dataFetcher(GraphQLFields.NAMESPACE, dataFetchingEnvironment -> dataFetchingEnvironment)
      .dataFetcher(GraphQLFields.APPLICATION, dataFetchingEnvironment -> dataFetchingEnvironment)
      .build();
  }

}
