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
import io.cdap.cdap.graphql.store.artifact.runtimewiring.ArtifactTypeRuntimeWiring;
import io.cdap.cdap.graphql.store.artifact.runtimewiring.QueryTypeRuntimeWiring;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

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
                             ArtifactTypeRuntimeWiring artifactTypeRuntimeWiring)
    throws IOException {
    String schemaDefinitionFile = "artifactSchema.graphqls";
    GraphQLProvider graphQLProvider = new ArtifactGraphQLProvider(schemaDefinitionFile,
                                                                  queryTypeRuntimeWiring,
                                                                  artifactTypeRuntimeWiring);
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
