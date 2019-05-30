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

package io.cdap.cdap.starwars.typeresolvers;

import graphql.TypeResolutionEnvironment;
import graphql.schema.GraphQLObjectType;
import graphql.schema.TypeResolver;

/**
 * TODO
 */
// TODO make class implement interface?
public class StarWarsTypeResolver {

  /**
   * TODO
   */
  // TODO change to instance metod?
  public static TypeResolver getCharacterTypeResolver() {
    return new TypeResolver() {
      @Override
      public GraphQLObjectType getType(TypeResolutionEnvironment typeResolutionEnvironment) {
        throw new UnsupportedOperationException("Implement");
      }
    };
  }
}
