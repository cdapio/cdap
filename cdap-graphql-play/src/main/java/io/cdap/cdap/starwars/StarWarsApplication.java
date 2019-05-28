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

package io.cdap.cdap.starwars;

import graphql.ExecutionResult;
import graphql.GraphQL;
import io.cdap.cdap.graphql.provider.GraphQLProvider;
import io.cdap.cdap.starwars.datafetchers.StarWarsDataFetchers;

import java.io.IOException;

/**
 * TODO
 */
public class StarWarsApplication {

  public static void main(String[] args) throws IOException {
    String schemaDefinitionFile = "starWarsSchema.graphqls";
    StarWarsDataFetchers starWarsDataFetchers = new StarWarsDataFetchers();
    GraphQLProvider starWarsGraphQLProvider = new StarWarsGraphQLProvider(schemaDefinitionFile, starWarsDataFetchers);
    GraphQL graphQL = starWarsGraphQLProvider.buildGraphQL();

    ExecutionResult executionResult = graphQL.execute("{human(id: \"1001\") {name }}");
    System.out.println(executionResult.getData().toString());
  }

}
