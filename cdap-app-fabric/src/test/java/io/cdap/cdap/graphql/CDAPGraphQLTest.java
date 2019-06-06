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

package io.cdap.cdap.graphql;

import graphql.GraphQL;
import io.cdap.cdap.graphql.cdap.provider.CDAPGraphQLProvider;
import io.cdap.cdap.graphql.cdap.schema.GraphQLSchemaFiles;
import io.cdap.cdap.graphql.provider.GraphQLProvider;
import io.cdap.cdap.graphql.store.application.schema.ApplicationSchemaFiles;
import io.cdap.cdap.graphql.store.artifact.schema.ArtifactSchemaFiles;
import io.cdap.cdap.graphql.store.namespace.schema.NamespaceSchemaFiles;
import io.cdap.cdap.graphql.store.programrecord.schema.ProgramRecordFields;
import io.cdap.cdap.graphql.store.programrecord.schema.ProgramRecordSchemaFiles;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.List;

public class CDAPGraphQLTest {

  protected static GraphQL graphQL;

  @BeforeClass
  public static void setup() throws Exception {
    List<String> schemaDefinitionFiles = Arrays.asList(GraphQLSchemaFiles.ROOT_SCHEMA,
                                                       ArtifactSchemaFiles.ARTIFACT_SCHEMA,
                                                       NamespaceSchemaFiles.NAMESPACE_SCHEMA,
                                                       ApplicationSchemaFiles.APPLICATION_SCHEMA,
                                                       ProgramRecordSchemaFiles.PROGRAM_RECORD_SCHEMA);
    GraphQLProvider graphQLProvider = new CDAPGraphQLProvider(schemaDefinitionFiles);
    graphQL = graphQLProvider.buildGraphQL();
  }

}
