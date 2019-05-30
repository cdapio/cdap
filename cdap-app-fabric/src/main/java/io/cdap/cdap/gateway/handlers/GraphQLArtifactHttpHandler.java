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
import io.cdap.cdap.graphql.provider.GraphQLProvider;
import io.cdap.cdap.graphql.store.artifact.ArtifactGraphQLProvider;
import io.cdap.cdap.graphql.store.artifact.runtimewiring.ArtifactDescriptorTypeRuntimeWiring;
import io.cdap.cdap.graphql.store.artifact.runtimewiring.ArtifactDetailTypeRuntimeWiring;
import io.cdap.cdap.graphql.store.artifact.runtimewiring.QueryTypeRuntimeWiring;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * TODO
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3)
public class GraphQLArtifactHttpHandler extends AbstractHttpHandler {

  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  private final GraphQL graphQL;

  @Inject
  GraphQLArtifactHttpHandler(QueryTypeRuntimeWiring queryTypeRuntimeWiring,
                             ArtifactDetailTypeRuntimeWiring artifactDetailTypeRuntimeWiring,
                             ArtifactDescriptorTypeRuntimeWiring artifactDescriptorTypeRuntimeWiring)
    throws IOException {
    String schemaDefinitionFile = "artifactSchema.graphqls";
    GraphQLProvider graphQLProvider = new ArtifactGraphQLProvider(schemaDefinitionFile,
                                                                  queryTypeRuntimeWiring,
                                                                  artifactDetailTypeRuntimeWiring,
                                                                  artifactDescriptorTypeRuntimeWiring);
    this.graphQL = graphQLProvider.buildGraphQL();
  }

  /**
   * TODO
   */
  @GET
  @Path("/namespaces/{namespace-id}/work")
  public void getArtifacts(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId) {

    String query = "{"
      + "  artifacts {"
      + "    name"
      + "    version"
      + "    scope"
      + "  }"
      + "}";

    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(query).build();
    CompletableFuture<ExecutionResult> promise = graphQL.executeAsync(executionInput);
    ExecutionResult executionResult = promise.join();

    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(executionResult.toSpecification()));
  }

}
