/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.security.AuditDetail;
import co.cask.cdap.common.security.AuditPolicy;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * The {@link HttpHandler} for handling REST calls to namespace endpoints.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class NamespaceHttpHandler extends AbstractAppFabricHttpHandler {

  private static final Gson GSON = new Gson();

  private final CConfiguration cConf;
  private final NamespaceAdmin namespaceAdmin;

  @Inject
  NamespaceHttpHandler(CConfiguration cConf, NamespaceAdmin namespaceAdmin) {
    this.cConf = cConf;
    this.namespaceAdmin = namespaceAdmin;
  }

  @GET
  @Path("/namespaces")
  public void getAllNamespaces(HttpRequest request, HttpResponder responder) throws Exception {
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(namespaceAdmin.list()));
  }

  @GET
  @Path("/namespaces/{namespace-id}")
  public void getNamespace(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId) throws Exception {
    NamespaceMeta ns = namespaceAdmin.get(new NamespaceId(namespaceId));
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(ns));
  }


  @PUT
  @Path("/namespaces/{namespace-id}/properties")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void updateNamespaceProperties(FullHttpRequest request, HttpResponder responder,
                                        @PathParam("namespace-id") String namespaceId) throws Exception {
    NamespaceMeta meta = getNamespaceMeta(request);
    namespaceAdmin.updateProperties(new NamespaceId(namespaceId), meta);
    responder.sendString(HttpResponseStatus.OK, String.format("Updated properties for namespace '%s'.", namespaceId));
  }

  @PUT
  @Path("/namespaces/{namespace-id}")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void create(FullHttpRequest request, HttpResponder responder,
                     @PathParam("namespace-id") String namespaceId) throws Exception {
    NamespaceId namespace;
    try {
      namespace = new NamespaceId(namespaceId);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException("Namespace id can contain only alphanumeric characters or '_'.");
    }

    NamespaceMeta metadata = getNamespaceMeta(request);

    if (isReserved(namespaceId)) {
      throw new BadRequestException(String.format("Cannot create the namespace '%s'. '%s' is a reserved namespace.",
                                                  namespaceId, namespaceId));
    }

    NamespaceMeta.Builder builder = metadata == null ? new NamespaceMeta.Builder() :
      new NamespaceMeta.Builder(metadata);
    builder.setName(namespace);

    NamespaceMeta finalMetadata = builder.build();

    try {
      namespaceAdmin.create(finalMetadata);
      responder.sendString(HttpResponseStatus.OK,
                           String.format("Namespace '%s' created successfully.", namespaceId));
    } catch (AlreadyExistsException e) {
      responder.sendString(HttpResponseStatus.OK, String.format("Namespace '%s' already exists.", namespaceId));
    }
  }

  @DELETE
  @Path("/unrecoverable/namespaces/{namespace-id}")
  public void delete(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespace)
    throws Exception {
    if (!cConf.getBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, Constants.Dangerous.DEFAULT_UNRECOVERABLE_RESET)) {
      responder.sendString(HttpResponseStatus.FORBIDDEN,
                           String.format("Namespace '%s' cannot be deleted because '%s' is not enabled. " +
                                           "Please enable it and restart CDAP Master.",
                                         namespace, Constants.Dangerous.UNRECOVERABLE_RESET));
      return;
    }
    NamespaceId namespaceId = new NamespaceId(namespace);
    namespaceAdmin.delete(namespaceId);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @DELETE
  @Path("/unrecoverable/namespaces/{namespace-id}/datasets")
  public void deleteDatasets(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespace) throws Exception {
    if (!cConf.getBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, Constants.Dangerous.DEFAULT_UNRECOVERABLE_RESET)) {
      responder.sendString(HttpResponseStatus.FORBIDDEN,
                           String.format("All datasets in namespace %s cannot be deleted because '%s' is not enabled." +
                                           " Please enable it and restart CDAP Master.",
                                         namespace, Constants.Dangerous.UNRECOVERABLE_RESET));
      return;
    }
    NamespaceId namespaceId = new NamespaceId(namespace);
    namespaceAdmin.deleteDatasets(namespaceId);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  private boolean isReserved(String namespaceId) {
    return NamespaceId.DEFAULT.getNamespace().equals(namespaceId)
      || NamespaceId.SYSTEM.getNamespace().equals(namespaceId)
      || NamespaceId.CDAP.getNamespace().equals(namespaceId);
  }

  private NamespaceMeta getNamespaceMeta(FullHttpRequest request) throws BadRequestException {
    try {
      return parseBody(request, NamespaceMeta.class);
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Invalid json object provided in request body.");
    }
  }
}
