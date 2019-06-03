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

package io.cdap.cdap.gateway.handlers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.graphql.cdap.provider.CDAPGraphQLProvider;
import io.cdap.cdap.graphql.cdap.runtimewiring.CDAPQueryTypeRuntimeWiring;
import io.cdap.cdap.graphql.cdap.schema.GraphQLSchemaFiles;
import io.cdap.cdap.graphql.provider.GraphQLProvider;
import io.cdap.cdap.graphql.store.artifact.runtimewiring.ArtifactQueryTypeRuntimeWiring;
import io.cdap.cdap.graphql.store.artifact.runtimewiring.ArtifactTypeRuntimeWiring;
import io.cdap.cdap.graphql.store.artifact.schema.ArtifactSchemaFiles;
import io.cdap.cdap.graphql.store.namespace.runtimewiring.NamespaceQueryTypeRuntimeWiring;
import io.cdap.cdap.graphql.store.namespace.schema.NamespaceSchemaFiles;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * {@link io.cdap.http.HttpHandler} for the GraphQL server
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3)
public class GraphQLHttpHandler extends AbstractHttpHandler {

  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  private final GraphQL graphQL;

  @Inject
  GraphQLHttpHandler(CDAPQueryTypeRuntimeWiring cdapQueryTypeRuntimeWiring,
                     ArtifactQueryTypeRuntimeWiring artifactQueryTypeRuntimeWiring,
                     ArtifactTypeRuntimeWiring artifactTypeRuntimeWiring,
                     NamespaceQueryTypeRuntimeWiring namespaceQueryTypeRuntimeWiring)
    throws IOException {
    List<String> schemaDefinitionFiles = Arrays.asList(
      GraphQLSchemaFiles.ROOT_SCHEMA,
      ArtifactSchemaFiles.ARTIFACT_SCHEMA,
      NamespaceSchemaFiles.NAMESPACE_SCHEMA);
    GraphQLProvider graphQLProvider = new CDAPGraphQLProvider(schemaDefinitionFiles,
                                                              cdapQueryTypeRuntimeWiring,
                                                              artifactQueryTypeRuntimeWiring,
                                                              artifactTypeRuntimeWiring,
                                                              namespaceQueryTypeRuntimeWiring);
    this.graphQL = graphQLProvider.buildGraphQL();
  }

  @POST
  @Path("/graphql")
  public void queryGraphQL(FullHttpRequest request, HttpResponder responder) {
    String query = request.content().toString(StandardCharsets.UTF_8);

    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(query).build();
    ExecutionResult executionResult = graphQL.execute(executionInput);

    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(executionResult.toSpecification()));
  }

}
