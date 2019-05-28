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

import graphql.ExecutionResult;
import graphql.GraphQL;
import io.cdap.cdap.graphql.provider.GraphQLProvider;
import io.cdap.cdap.standalone.datafetchers.BooksDataFetchers;

import java.io.IOException;

public class BooksApplication {

  public static void main(String[] args) throws IOException {
    String schemaDefinitionFile = "schema.graphqls";
    BooksDataFetchers booksDataFetchers = new BooksDataFetchers();
    GraphQLProvider booksGraphQLProvider = new BooksGraphQLProvider(schemaDefinitionFile, booksDataFetchers);
    GraphQL graphQL = booksGraphQLProvider.buildGraphQL();

    // ExecutionResult executionResult = graphQL.execute("{bookById(id: \"book-1\") {name author {firstName } }}");
    ExecutionResult executionResult = graphQL.execute("{bookById(id: \"book-1\") {name }}");
    System.out.println(executionResult.getData().toString());
  }

}
