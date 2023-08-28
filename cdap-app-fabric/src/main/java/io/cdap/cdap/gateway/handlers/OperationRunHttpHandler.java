/*
 * Copyright © 2014-2019 Cask Data, Inc.
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
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import io.cdap.cdap.api.service.operation.OperationError;
import io.cdap.cdap.api.service.operation.OperationMeta;
import io.cdap.cdap.api.service.operation.OperationRun;
import io.cdap.cdap.api.service.operation.OperationStatus;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.security.AuditDetail;
import io.cdap.cdap.common.security.AuditPolicy;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.internal.operations.OperationRunsStore;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * The {@link HttpHandler} for handling REST calls to namespace endpoints.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}/operations")
public class OperationRunHttpHandler extends AbstractAppFabricHttpHandler {

  private static final Gson GSON = new Gson();

  private final CConfiguration cConf;
  private final OperationRunsStore store;

  @Inject
  OperationRunHttpHandler(CConfiguration cConf, OperationRunsStore store) {
    this.cConf = cConf;
    this.store = store;
  }


  @GET
  @Path("/")
  public void listOperations(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId) throws Exception {
      List<OperationRun> runs = store.listOperations(new NamespaceId(namespaceId));
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(runs));
  }


  @GET
  @Path("/{operation-id}")
  public void getNamespace(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId, @PathParam("operation-id") String operationId) throws Exception {
    OperationRun run = store.getOperation(new NamespaceId(namespaceId), operationId);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(run));
  }


  @POST
  @Path("/{operation-id}/updateMeta")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void updateOperationMeta(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId, @PathParam("operation-id") String operationId) throws Exception {
    try {
      OperationMeta meta = parseBody(request, OperationMeta.class);
      store.updateOperationMeta(new NamespaceId(namespaceId), operationId, meta);
      responder.sendString(HttpResponseStatus.OK,
                           String.format("Updated meta for operation %s in namespace '%s'.", operationId, namespaceId));
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Invalid json object provided in request body.");
    }
  }

  @POST
  @Path("/{operation-id}/updateStatus")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void updateOperationStatus(FullHttpRequest request, HttpResponder responder,
                                        @PathParam("namespace-id") String namespaceId, @PathParam("operation-id") String operationId) throws Exception {
    try {
      OperationStatus status = parseBody(request, OperationStatus.class);
      store.updateOperationStatus(new NamespaceId(namespaceId), operationId, status);
      responder.sendString(HttpResponseStatus.OK,
                           String.format("Updated status for operation %s in namespace '%s'.", operationId, namespaceId));
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Invalid json object provided in request body.");
    }
  }

  @POST
  @Path("/{operation-id}/stop")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void failOperation(FullHttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId, @PathParam("operation-id") String operationId) throws Exception {
    try {
      List<OperationError> errors = parseBody(request, OperationRunsStore.LIST_ERROR_TYPE);
      store.failOperation(new NamespaceId(namespaceId), operationId, errors);
      responder.sendString(HttpResponseStatus.OK,
                           String.format("Updated status for operation %s in namespace '%s'.", operationId, namespaceId));
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Invalid json object provided in request body.");
    }
  }
}
