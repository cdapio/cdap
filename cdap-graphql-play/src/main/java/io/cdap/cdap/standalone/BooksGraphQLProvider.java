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
import io.cdap.cdap.graphql.schema.Types;
import io.cdap.cdap.standalone.datafetchers.BooksDataFetchers;
import io.cdap.cdap.standalone.schema.BooksFields;
import io.cdap.cdap.standalone.schema.BooksTypes;

/**
 * TODO
 */
class BooksGraphQLProvider extends AbstractGraphQLProvider {

  private final BooksDataFetchers booksDataFetchers;

  BooksGraphQLProvider(String schemaDefinitionFile, BooksDataFetchers booksDataFetchers) {
    super(schemaDefinitionFile);

    this.booksDataFetchers = booksDataFetchers;
  }

  protected RuntimeWiring buildWiring() {
    return RuntimeWiring.newRuntimeWiring()
      .type(queryTypeRuntimeWiring())
      .type(bookTypeRuntimeWiring())
      .build();
  }

  // TODO use static final strings that map to the schema
  private TypeRuntimeWiring.Builder bookTypeRuntimeWiring() {
    return TypeRuntimeWiring.newTypeWiring(BooksTypes.BOOK)
      .dataFetcher(BooksFields.AUTHOR, booksDataFetchers.getAuthorDataFetcher());
  }

  private TypeRuntimeWiring.Builder queryTypeRuntimeWiring() {
    return TypeRuntimeWiring.newTypeWiring(Types.QUERY)
      .dataFetcher(BooksFields.BOOK_BY_ID, booksDataFetchers.getBookByIdDataFetcher());
  }
}
