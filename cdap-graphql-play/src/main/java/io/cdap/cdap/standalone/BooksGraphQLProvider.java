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

package io.cdap.cdap.standalone;

import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.TypeRuntimeWiring;
import io.cdap.cdap.graphql.provider.AbstractGraphQLProvider;

import java.io.IOException;

import static graphql.schema.idl.TypeRuntimeWiring.newTypeWiring;

/**
 * TODO
 */
class BooksGraphQLProvider extends AbstractGraphQLProvider {

  BooksGraphQLProvider(String schemaDefinitionFile) throws IOException {
    super(schemaDefinitionFile);
  }

  protected RuntimeWiring buildWiring() {
    return RuntimeWiring.newRuntimeWiring()
      .type(queryTypeRuntimeWiring())
      .type(bookTypeRuntimeWiring())
      .build();
  }

  private TypeRuntimeWiring.Builder bookTypeRuntimeWiring() {
    return newTypeWiring("Book")
      .dataFetcher("author", GraphQLDataFetchers.getAuthorDataFetcher());
  }

  private TypeRuntimeWiring.Builder queryTypeRuntimeWiring() {
    return newTypeWiring("Query")
      .dataFetcher("bookById", GraphQLDataFetchers.getBookByIdDataFetcher());
  }
}
