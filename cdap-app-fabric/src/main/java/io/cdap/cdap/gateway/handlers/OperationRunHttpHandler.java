/*
 * Copyright © 2023 Cask Data, Inc.
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
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.proto.operationrun.OperationRun;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * The {@link HttpHandler} for handling REST calls to namespace endpoints.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}/operations")
public class OperationRunHttpHandler extends AbstractAppFabricHttpHandler {

  private static final Gson GSON = new Gson();

  @Inject
  OperationRunHttpHandler() {
  }

  /**
   * API to fetch all running operations in a namespace.
   *
   * @param namespaceId Namespace to fetch runs from
   * @param pageToken the token identifier for the current page requested in a paginated
   *     request
   * @param pageSize the number of application details returned in a paginated request
   * @param filter optional filters in EBNF grammar. Currently Only one status and one type
   *     filter is supported with AND expression.
   */
  @GET
  @Path("/")
  public void scanOperations(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("pageSize") Integer pageSize,
      @QueryParam("filter") String filter) {
    // TODO(samik, CDAP-20812) fetch the operation runs from store
    List<OperationRun> runs = new ArrayList<>();
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(runs));
  }

  /**
   * API to fetch operation run by id.
   *
   * @param namespaceId Namespace to fetch runs from
   * @param runId id of the operation run
   */
  @GET
  @Path("/{id}")
  public void getOperationRun(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("id") String runId) {
    // // TODO(samik, CDAP-20813) fetch the operation runs from store
    OperationRun run = null;
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(run));
  }

  /**
   * API to stop operation run by id.
   *
   * @param namespaceId Namespace to fetch runs from
   * @param runId id of the operation run
   */
  @POST
  @Path("/{id}/stop")
  public void failOperation(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("id") String runId) {
    // // TODO(samik, CDAP-20814) send the message to stop the operation
    responder.sendString(HttpResponseStatus.OK,
        String.format("Updated status for operation run %s in namespace '%s'.", runId,
            namespaceId));
  }
}
