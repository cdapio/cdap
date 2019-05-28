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

import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.TypeRuntimeWiring;
import io.cdap.cdap.graphql.provider.AbstractGraphQLProvider;
import io.cdap.cdap.graphql.schema.Types;
import io.cdap.cdap.starwars.datafetchers.StarWarsDataFetchers;
import io.cdap.cdap.starwars.schema.StarWarsFields;
import io.cdap.cdap.starwars.schema.StarWarsInterfaces;
import io.cdap.cdap.starwars.schema.StarWarsTypes;
import io.cdap.cdap.starwars.typeresolvers.StarWarsTypeResolver;

class StarWarsGraphQLProvider extends AbstractGraphQLProvider {

  private final StarWarsDataFetchers starWarsDataFetchers;

  StarWarsGraphQLProvider(String schemaDefinitionFile, StarWarsDataFetchers starWarsDataFetchers) {
    super(schemaDefinitionFile);

    this.starWarsDataFetchers = starWarsDataFetchers;
  }

  @Override
  protected RuntimeWiring buildWiring() {
    return RuntimeWiring.newRuntimeWiring()
      .type(getQueryTypeRuntimeWiring())
      .type(getCharacterTypeRuntimeWiring())
      .type(getHumanTypeRuntimeWiring())
      .type(getDroidTypeRuntimeWiring())
      .build();
  }

  private TypeRuntimeWiring getQueryTypeRuntimeWiring() {
    return TypeRuntimeWiring.newTypeWiring(Types.QUERY)
      .dataFetcher(StarWarsFields.HERO, starWarsDataFetchers.getHeroDataFetcher())
      .dataFetcher(StarWarsFields.HUMAN, starWarsDataFetchers.getHumanDataFetcher())
      .dataFetcher(StarWarsFields.DROID, starWarsDataFetchers.getDroidDataFetcher())
      .build();
  }

  private TypeRuntimeWiring getCharacterTypeRuntimeWiring() {
    return TypeRuntimeWiring.newTypeWiring(StarWarsInterfaces.CHARACTER)
      .typeResolver(StarWarsTypeResolver.getCharacterTypeResolver())
      .build();
  }

  private TypeRuntimeWiring getHumanTypeRuntimeWiring() {
    return TypeRuntimeWiring.newTypeWiring(StarWarsTypes.HUMAN)
      .build();
  }

  private TypeRuntimeWiring getDroidTypeRuntimeWiring() {
    return TypeRuntimeWiring.newTypeWiring(StarWarsTypes.DROID)
      .build();
  }

}
