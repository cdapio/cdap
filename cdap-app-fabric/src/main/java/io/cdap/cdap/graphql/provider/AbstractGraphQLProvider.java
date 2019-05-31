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

package io.cdap.cdap.graphql.provider;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract class that implements {@link GraphQLProvider} to create a GraphQL server
 */
public abstract class AbstractGraphQLProvider implements GraphQLProvider {

  private final List<String> schemaDefinitionFiles;

  protected AbstractGraphQLProvider(List<String> schemaDefinitionFiles) {
    this.schemaDefinitionFiles = schemaDefinitionFiles;
  }

  @Override
  public GraphQL buildGraphQL() throws IOException {
    GraphQLSchema graphQLSchema = buildSchema();

    return GraphQL.newGraphQL(graphQLSchema).build();
  }

  private GraphQLSchema buildSchema() throws IOException {
    List<String> schemaFiles = loadSchemaFiles();
    TypeDefinitionRegistry typeDefinitionRegistry = parseSchemas(schemaFiles);

    SchemaGenerator schemaGenerator = new SchemaGenerator();
    RuntimeWiring runtimeWiring = buildWiring();

    return schemaGenerator.makeExecutableSchema(typeDefinitionRegistry, runtimeWiring);
  }

  private TypeDefinitionRegistry parseSchemas(List<String> schemaFiles) {
    SchemaParser schemaParser = new SchemaParser();
    TypeDefinitionRegistry typeDefinitionRegistry = new TypeDefinitionRegistry();

    for (String schemaFile : schemaFiles) {
      typeDefinitionRegistry.merge(schemaParser.parse(schemaFile));
    }

    return typeDefinitionRegistry;
  }

  private List<String> loadSchemaFiles() throws IOException {
    List<String> schemaFiles = new ArrayList<>();

    for (String schemaFile : schemaDefinitionFiles) {
      schemaFiles.add(loadSchema(schemaFile));
    }

    return schemaFiles;
  }

  private String loadSchema(String s) throws IOException {
    URL url = Resources.getResource(s);

    return Resources.toString(url, Charsets.UTF_8);
  }

  /**
   * Builds the runtime wiring by registering data fetchers
   */
  protected abstract RuntimeWiring buildWiring();
}
